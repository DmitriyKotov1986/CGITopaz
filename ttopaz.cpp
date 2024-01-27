#include "ttopaz.h"

//QT
#include <QSqlError>
#include <QSqlQuery>
#include <QXmlStreamReader>

//My
#include "Common/common.h"

using namespace CGITopaz;

using namespace Common;

TTopaz::TTopaz(CGITopaz::TConfig* cfg)
{
    Q_ASSERT(cfg != nullptr);

    //настраиваем БД
    _db = QSqlDatabase::addDatabase(cfg->db_Driver(), "MainDB");
    _db.setDatabaseName(cfg->db_DBName());
    _db.setUserName(cfg->db_UserName());
    _db.setPassword(cfg->db_Password());
    _db.setConnectOptions(cfg->db_ConnectOptions());
    _db.setPort(cfg->db_Port());
    _db.setHostName(cfg->db_Host());
}

TTopaz::~TTopaz()
{
    if (_db.isOpen())
    {
        _db.close();
    }
}

int TTopaz::run(const QString& XMLText)
{
    writeDebugLogFile("REQUEST>", XMLText);

    QTextStream textStream(stderr);

    if (XMLText.isEmpty())
    {
        _errorString = "XML is empty";
        textStream << _errorString;
        return EXIT_CODE::XML_EMPTY;
    }

    if (!_db.open())
    {
        _errorString = "Cannot connet to DB. Error: " + _db.lastError().text();
        return EXIT_CODE::SQL_NOT_OPEN_DB;
    } 

    //парсим XML
    QXmlStreamReader XMLReader(XMLText);
    QString AZSCode = "n/a";
    QString clientVersion = "n/a";
    QString protocolVersion = "n/a";
    TDocsInfoList docs;
    quint64 lastQueryID = 0;  //ID последнего обработанного запроса

    while ((!XMLReader.atEnd()) && (!XMLReader.hasError()))
    {
        const auto token = XMLReader.readNext();
        if (token == QXmlStreamReader::StartDocument)
        {
            continue;
        }
        else if (token == QXmlStreamReader::EndDocument)
        {
            break;
        }
        else if (token == QXmlStreamReader::StartElement)
        {
            //qDebug() << XMLReader.name().toString();
            if (XMLReader.name().toString()  == "Root")
            {
                while ((XMLReader.readNext() != QXmlStreamReader::EndElement) && !XMLReader.atEnd() && !XMLReader.hasError())
                {
                    //qDebug() << "Root/" << XMLReader.name().toString();
                    if (XMLReader.name().toString().isEmpty())
                    {
                        continue;
                    }
                    if (XMLReader.name().toString()  == "AZSCode")
                    {
                        AZSCode = XMLReader.readElementText();
                    }
                    else if (XMLReader.name().toString()  == "ClientVersion")
                    {
                        clientVersion = XMLReader.readElementText();
                    }
                    else if (XMLReader.name().toString()  == "ProtocolVersion")
                    {
                        protocolVersion = XMLReader.readElementText();
                    }
                    else if (XMLReader.name().toString()  == "LastQueryID")
                    {
                        bool ok = false;

                        lastQueryID = XMLReader.readElementText().toULongLong(&ok);
                        if (!ok)
                        {
                            _errorString = "Incorrect value tag (LastQueryID" + XMLReader.name().toString() + "). Must be number";
                            textStream << _errorString; //выводим сообщение об ошибке в cerr для отправки клиенту
                            return EXIT_CODE::XML_PARSE_ERR;
                        }
                    }

                    else if (XMLReader.name().toString()  == "Document")
                    {
                        TDocInfo doc;
                        while ((XMLReader.readNext() != QXmlStreamReader::EndElement) && !XMLReader.atEnd() && !XMLReader.hasError())
                        {
                            if (XMLReader.name().toString().isEmpty())
                            {
                                continue;
                            }
                            else if (XMLReader.name().toString()  == "DocumentType")
                            {
                                doc.type = XMLReader.readElementText();
                            }
                            else if (XMLReader.name().toString()  == "DateTime")
                            {
                                doc.dateTime = QDateTime::fromString(XMLReader.readElementText(), "yyyy-MM-dd hh:mm:ss.zzz");
                            }
                            else if (XMLReader.name().toString()  == "DocumentNumber")
                            {
                                bool ok = true;
                                doc.number = XMLReader.readElementText().toInt(&ok);
                                if (!ok)
                                {
                                    _errorString = "Incorrect value tag (DocumentNumber" + XMLReader.name().toString() + "). Must be number";
                                    textStream << _errorString; //выводим сообщение об ошибке в cerr для отправки клиенту
                                    return EXIT_CODE::XML_PARSE_ERR;
                                }
                            }
                            else if (XMLReader.name().toString()  == "Smena")
                            {
                                bool ok = true;
                                doc.smena = XMLReader.readElementText().toInt(&ok);
                                if (!ok)
                                {
                                    _errorString = "Incorrect value tag (Smena" + XMLReader.name().toString() + "). Must be number";
                                    textStream << _errorString; //выводим сообщение об ошибке в cerr для отправки клиенту
                                    return EXIT_CODE::XML_PARSE_ERR;
                                }
                            }
                            else if (XMLReader.name().toString()  == "Creater")
                            {
                                doc.creater = XMLReader.readElementText();
                            }
                            else if (XMLReader.name().toString()  == "Body")
                            {
                                doc.Body = XMLReader.readElementText();
                            }

                            else
                            {
                                _errorString = "Undefine tag in XML (Document/" + XMLReader.name().toString() + ")";
                                textStream << _errorString; //выводим сообщение об ошибке в cerr для отправки клиенту
                                return EXIT_CODE::XML_UNDEFINE_TOCKEN;
                            }
                        }
                        docs.push_back(doc);
                    }
                    else
                    {
                        _errorString = "Undefine tag in XML (Root/" + XMLReader.name().toString() + ")";
                        textStream << _errorString; //выводим сообщение об ошибке в cerr для отправки клиенту
                        return EXIT_CODE::XML_UNDEFINE_TOCKEN;
                    }
                }
            }
        }
        else
        {
            _errorString = "Undefine token in XML";
            textStream << _errorString; //выводим сообщение об ошибке в cin для отправки клиенту
            return EXIT_CODE::XML_UNDEFINE_TOCKEN;
        }
    }

    if (XMLReader.hasError()) //неудалось распарсить пришедшую XML
    {
        _errorString = "Cannot parse XML query. Message: " + XMLReader.errorString();
        textStream << _errorString; //выводим сообщение об ошибке в cin для отправки клиенту
        return EXIT_CODE::XML_PARSE_ERR;
    }

    if (!docs.isEmpty())
    {
        QString docsValues;
        for (const auto& docItem: docs)
        {
            if (!docsValues.isEmpty())
            {
                docsValues += ", ";
            }
            docsValues += QString("'%1', CAST('%2' AS datetime2), '%3', %4, %5, '%6', '%7'")
                            .arg(AZSCode)
                            .arg(docItem.dateTime.toString("yyyy-MM-dd hh:mm:ss.zzz"))
                            .arg(docItem.type)
                            .arg(docItem.number)
                            .arg(docItem.smena)
                            .arg(docItem.creater)
                            .arg(docItem.Body);
        }

        QString queryText = QString("INSERT INTO [TopazDocuments] ([AZSCode], [DateTime], [DocumentType], [DocumentNumber], [Smena], [Creater], [Body]) "
                                    "VALUES (%1) ").arg(docsValues);

        DBQueryExecute(_db, queryText);
    }

    //Формируем ответ
    QString answer;
    QXmlStreamWriter XMLWriter(&answer);
    XMLWriter.setAutoFormatting(true);
    XMLWriter.writeStartDocument("1.0");
    XMLWriter.writeStartElement("Root");
    XMLWriter.writeTextElement("ProtocolVersion", "0.1");
    XMLWriter.writeTextElement("LoadStatus", "1");

    //формируем список невыполненных запросов
    QString queryText;
    if (lastQueryID == 0)
    {
        queryText = QString("SELECT [ID], [QueryID], [QueryText] "
                            "FROM [QueriesToTopaz] "
                            "WHERE [AZSCode] = %1 AND [LoadFromDateTime] IS NULL "
                            "ORDER BY [ID]")
                        .arg(AZSCode);
    }
    else //lastQueryID !=  0
    {
        queryText = QString("SELECT [ID], [QueryID], [QueryText] "
                            "FROM [QueriesToTopaz] "
                            "WHERE [AZSCode] = %1 AND [ID] > %2 "
                            "ORDER BY [ID]")
                        .arg(AZSCode)
                        .arg(lastQueryID);
    }

    writeDebugLogFile(QString("QUERY TO %1>").arg(_db.connectionName()), queryText);

    QSqlQuery query(_db);
    if (!query.exec(queryText))
    {
        errorDBQuery(_db, query);
    }

    XMLWriter.writeStartElement("Queries");

    while (query.next())
    {
        XMLWriter.writeStartElement("Query");
        XMLWriter.writeTextElement("QueryID", query.value("QueryID").toString());
        XMLWriter.writeTextElement("ID", query.value("ID").toString());
        XMLWriter.writeTextElement("QueryText", query.value("QueryText").toString());
        XMLWriter.writeEndElement(); //Query
    }

    XMLWriter.writeEndElement();  //Queries
    XMLWriter.writeEndElement();  //root
    XMLWriter.writeEndDocument();

    DBCommit(_db);

    //отмечаем все выполненные запросы
    QStringList executedQueryIDList;
    for (const auto& doc: docs)
    {
        if (doc.type == "Query")
        {
           executedQueryIDList.push_back(QString::number(doc.number));
        }
    }

    if (!executedQueryIDList.isEmpty())
    {
        queryText = QString("UPDATE [QueriesToTopaz] "
                            "SET [LoadFromDateTime] = CAST('%1' AS DATETIME2) "
                            "WHERE [ID] IN (%2)")
                        .arg(QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss.zzz"))
                        .arg(executedQueryIDList.join(','));

        DBQueryExecute(_db, queryText);
    }

    //отправляем ответ
    QTextStream answerTextStream(stdout);
    answerTextStream << answer;

    writeDebugLogFile(QString("ANSWER>"), answer);

    return EXIT_CODE::OK;
}
