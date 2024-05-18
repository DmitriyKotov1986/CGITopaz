#include "ttopaz.h"

//STL
#include <stdexcept>

//QT
#include <QSqlError>
#include <QSqlQuery>
#include <QXmlStreamReader>

//My
#include "Common/common.h"
#include "tconfig.h"

using namespace CGITopaz;
using namespace Common;

static const QString QUERY_TYPE_QUERY = "Query";
static const QString QUERY_TYPE_SESSION_REPORT = "SessionReport";
static const QString QUERY_TYPE_SESSION_DATA = "SessionData";
static const QString CURRENT_PROTOCOL_VERSION = "0.1";

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

    QTextStream errStream(stderr);

    if (XMLText.isEmpty())
    {
        _errorString = "XML is empty";
        errStream << _errorString;
        return EXIT_CODE::XML_EMPTY;
    }

    //парсим XML
    QXmlStreamReader XMLReader(XMLText);

    QString AZSCode;
    QString clientVersion;
    QString protocolVersion;

    TDocsInfoList docs;

    quint64 lastQuerySQLID = 0;  //ID последнего обработанного запроса
    quint64 lastQuerySessionReportsID = 0; //ID последнего запрошенного сменного отчета
    quint64 lastQuerySessionsDataID = 0; //ID последнего запрошенного сменного отчета

    try
    {
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
               // qDebug() << XMLReader.name().toString();
                if (XMLReader.name().toString()  == "Root")
                {
                    while ((XMLReader.readNext() != QXmlStreamReader::EndElement) && !XMLReader.atEnd() && !XMLReader.hasError())
                    {
                      //  qDebug() << "Root/" << XMLReader.name().toString();
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
                        else if (XMLReader.name().toString() == "LastQueryID")
                        {
                            bool ok = false;
                            lastQuerySQLID = XMLReader.readElementText().toULongLong(&ok);
                            if (!ok)
                            {
                                throw std::runtime_error(QString("Incorrect value tag (Root/%1). Value: %2. Value must be number")
                                        .arg(XMLReader.name().toString())
                                        .arg(XMLReader.readElementText()).toStdString());
                            }
                        }
                        else if (XMLReader.name().toString() == "LastQuerySessionReportsID")
                        {
                            bool ok = false;
                            lastQuerySessionReportsID = XMLReader.readElementText().toULongLong(&ok);
                            if (!ok)
                            {
                                throw std::runtime_error(QString("Incorrect value tag (Root/%1). Value: %2. Value must be number")
                                        .arg(XMLReader.name().toString())
                                        .arg(XMLReader.readElementText()).toStdString());
                            }
                        }
                        else if (XMLReader.name().toString() == "LastQuerySessionsDataID")
                        {
                            bool ok = false;
                            lastQuerySessionsDataID = XMLReader.readElementText().toULongLong(&ok);
                            if (!ok)
                            {
                                throw std::runtime_error(QString("Incorrect value tag (Root/%1). Value: %2. Value must be number")
                                        .arg(XMLReader.name().toString())
                                        .arg(XMLReader.readElementText()).toStdString());
                            }
                        }
                        else if (XMLReader.name().toString()  == "Documents")
                        {
                            docs = parseDocuments(XMLReader);
                        }
                        else
                        {
                            throw std::runtime_error(QString("Undefine tag in XML (Root/%1)")
                                                        .arg(XMLReader.name().toString()).toStdString());
                        }
                    }
                }
                else
                {
                    throw std::runtime_error(QString("Undefine tag in XML (%1)")
                                                .arg(XMLReader.name().toString()).toStdString());
                }
            }
            else
            {
                throw std::runtime_error(QString("Undefine token in XML").toStdString());
            }
        }

        if (XMLReader.hasError()) //неудалось распарсить пришедшую XML
        {
            throw std::runtime_error(QString("Cannot parse XML query. Message: %1")
                                        .arg(XMLReader.errorString()).toStdString());
        }

        if (protocolVersion.isEmpty()  || protocolVersion != CURRENT_PROTOCOL_VERSION)
        {
            throw std::runtime_error(QString("Value tag Root/%1 cannot be empty or protocol version is not support. Value: %2")
                                        .arg(XMLReader.name().toString()
                                        .arg(protocolVersion)).toStdString());
        }
        if (AZSCode.isEmpty())
        {
            throw std::runtime_error(QString("Value tag Root/%1 cannot be empty")
                                        .arg(XMLReader.name().toString()).toStdString());
        }
    }
    catch (std::exception &err)
    {
        _errorString = QString("Error parse XML: %1").arg(err.what());
        errStream << _errorString;

        return EXIT_CODE::XML_PARSE_ERR;
    }

    connectToDB();
    _db.transaction();

    saveDocs(AZSCode, docs);

    //Формируем ответ
    QString answer;
    QXmlStreamWriter XMLWriter(&answer);
    XMLWriter.setAutoFormatting(true);
    XMLWriter.writeStartDocument("1.0");
    XMLWriter.writeStartElement("Root");
    XMLWriter.writeTextElement("ProtocolVersion", CURRENT_PROTOCOL_VERSION);

    addQuerySQL(XMLWriter, AZSCode, lastQuerySQLID);
    addQuerySessionReports(XMLWriter, AZSCode, lastQuerySessionReportsID);
    addQuerySessionsData(XMLWriter, AZSCode, lastQuerySessionsDataID);

    XMLWriter.writeEndElement();  //root
    XMLWriter.writeEndDocument();

    markCompliteQuerySQL(docs, AZSCode);
    markCompliteQuerySessionReports(docs, AZSCode);
    markCompliteQuerySessionsData(docs, AZSCode);

    DBCommit(_db);

    //отправляем ответ
    QTextStream answerTextStream(stdout);
    answerTextStream << answer;

    writeDebugLogFile(QString("ANSWER>"), answer);

    return EXIT_CODE::OK;
}

void TTopaz::connectToDB()
{
    const auto cnf = TConfig::config();
    Q_CHECK_PTR(cnf);

    //настраиваем подключениек БД
    if (!Common::connectToDB(_db, cnf->db_ConnectionInfo(), "MainDB"))
    {
        QString msg = connectDBErrorString(_db);
        qCritical() << QString("%1 %2").arg(QTime::currentTime().toString(SIMPLY_TIME_FORMAT)).arg(msg);
        Common::writeLogFile("ERR>", msg);

        exit(EXIT_CODE::SQL_NOT_CONNECT);
    }
}

void TTopaz::saveDocs(const QString& AZSCode, const TDocsInfoList& docs)
{
    if (!docs.isEmpty())
    {
        QString docsValues;
        for (const auto& docItem: docs)
        {

            QString queryText = QString("INSERT INTO [TopazDocuments] ([AZSCode], [DateTime], [QueryID], [DocumentType], [DocumentNumber], [Smena], [Creater], [Body]) "
                                        "VALUES ('%1', CAST('%2' AS datetime2), '%3', '%4', %5, %6, '%7', '%8') ")
                                    .arg(AZSCode)
                                    .arg(docItem.dateTime.toString(DATETIME_FORMAT))
                                    .arg(docItem.queryID)
                                    .arg(docItem.type)
                                    .arg(docItem.number)
                                    .arg(docItem.smena)
                                    .arg(docItem.creater)
                                    .arg(docItem.body);

            writeDebugLogFile(QString("QUERY TO %1>").arg(_db.connectionName()), queryText);

            QSqlQuery query(_db);
            if (!query.exec(queryText))
            {
                errorDBQuery(_db, query);
            }
        }
    }
}

void TTopaz::updateSentToDateTime(const QString &AZSCode, const QString &tableName, const QStringList &executedQueryIDList)
{
    if (executedQueryIDList.isEmpty())
    {
        return;
    }

    const QString queryText =
            QString("UPDATE [%1] "
                    "SET [SentToDateTime] = CAST('%2' AS DATETIME2) "
                    "WHERE [QueryID] IN (%3) AND [AZSCode] = '%4'")
                .arg(tableName)
                .arg(QDateTime::currentDateTime().toString(DATETIME_FORMAT))
                .arg(executedQueryIDList.join(','))
                .arg(AZSCode);

    writeDebugLogFile(QString("QUERY TO %1>").arg(_db.connectionName()), queryText);

    QSqlQuery query(_db);
    if (!query.exec(queryText))
    {
        errorDBQuery(_db, query);
    }
}

void TTopaz::addQuerySQL(QXmlStreamWriter &XMLWriter, const QString& AZSCode, quint64 lastQueryID)
{
    //формируем список невыполненных запросов
    QString queryText;
    if (lastQueryID == 0)
    {
        queryText = QString("SELECT [ID], [QueryID], [QueryText] "
                            "FROM [QueriesToTopaz] "
                            "WHERE [AZSCode] = '%1' AND [LoadFromDateTime] IS NULL "
                            "ORDER BY [ID]")
                        .arg(AZSCode);
    }
    else
    {
        queryText = QString("SELECT [ID], [QueryID], [QueryText] "
                            "FROM [QueriesToTopaz] "
                            "WHERE [AZSCode] = '%1' AND [ID] > %2 "
                            "ORDER BY [ID]")
                        .arg(AZSCode)
                        .arg(lastQueryID);
    }

    writeDebugLogFile(QString("QUERY TO %1>").arg(_db.connectionName()), queryText);

    QSqlQuery query(_db);
    query.setForwardOnly(true);

    if (!query.exec(queryText))
    {
        errorDBQuery(_db, query);
    }

    QStringList executedQueryIDList;

    XMLWriter.writeStartElement("Queries");

    while (query.next())
    {
        const QString queryId = query.value("QueryID").toString();

        XMLWriter.writeStartElement(QUERY_TYPE_QUERY);

        XMLWriter.writeTextElement("QueryID", queryId);
        XMLWriter.writeTextElement("ID", query.value("ID").toString());
        XMLWriter.writeTextElement("QueryText", query.value("QueryText").toString());

        XMLWriter.writeEndElement(); //Query

        executedQueryIDList.push_back(QString("'%1'").arg(queryId));
    }

    XMLWriter.writeEndElement();  //Queries

    updateSentToDateTime(AZSCode, "QueriesToTopaz",  executedQueryIDList);
}

void TTopaz::addQuerySessionReports(QXmlStreamWriter &XMLWriter, const QString &AZSCode, quint64 lastQueryID)
{
    //формируем список невыполненных запросов
    QString queryText;
    if (lastQueryID == 0)
    {
        queryText = QString("SELECT [ID], [SessionNum], [QueryID] "
                            "FROM [QueriesSessionReports] "
                            "WHERE [AZSCode] = '%1' AND [LoadFromDateTime] IS NULL "
                            "ORDER BY [ID]")
                        .arg(AZSCode);
    }
    else
    {
        queryText = QString("SELECT [ID], [SessionNum], [QueryID] "
                            "FROM [QueriesSessionReports] "
                            "WHERE [AZSCode] = '%1' AND [ID] > %2 "
                            "ORDER BY [ID]")
                        .arg(AZSCode)
                        .arg(lastQueryID);
    }

    writeDebugLogFile(QString("QUERY TO %1>").arg(_db.connectionName()), queryText);

    QSqlQuery query(_db);
    query.setForwardOnly(true);

    if (!query.exec(queryText))
    {
        errorDBQuery(_db, query);
    }

    QStringList executedQueryIDList;

    XMLWriter.writeStartElement("SessionReports");

    while (query.next())
    {
        const QString queryId = query.value("QueryID").toString();

        XMLWriter.writeStartElement(QUERY_TYPE_SESSION_REPORT);

        XMLWriter.writeTextElement("ID", query.value("ID").toString());
        XMLWriter.writeTextElement("QueryID", queryId);
        XMLWriter.writeTextElement("SessionNum", query.value("SessionNum").toString());

        XMLWriter.writeEndElement(); //SessionReport

        executedQueryIDList.push_back(QString("'%1'").arg(queryId));
    }

    XMLWriter.writeEndElement();  //SessionReports

    updateSentToDateTime(AZSCode, "QueriesSessionReports",  executedQueryIDList);
}

void TTopaz::addQuerySessionsData(QXmlStreamWriter &XMLWriter, const QString &AZSCode, quint64 lastQueryID)
{
    //формируем список невыполненных запросов
    QString queryText;
    if (lastQueryID == 0)
    {
        queryText = QString("SELECT [ID], [Count], [QueryID] "
                            "FROM [QueriesSessionsData] "
                            "WHERE [AZSCode] = '%1' AND [LoadFromDateTime] IS NULL "
                            "ORDER BY [ID]")
                        .arg(AZSCode);
    }
    else
    {
        queryText = QString("SELECT [ID], [Count], [QueryID] "
                            "FROM [QueriesSessionsData] "
                            "WHERE [AZSCode] = '%1' AND [ID] > %2 "
                            "ORDER BY [ID]")
                        .arg(AZSCode)
                        .arg(lastQueryID);
    }

    writeDebugLogFile(QString("QUERY TO %1>").arg(_db.connectionName()), queryText);

    QSqlQuery query(_db);
    query.setForwardOnly(true);

    if (!query.exec(queryText))
    {
        errorDBQuery(_db, query);
    }

        QStringList executedQueryIDList;
    XMLWriter.writeStartElement("SessionsData");

    while (query.next())
    {
        const QString queryId = query.value("QueryID").toString();

        XMLWriter.writeStartElement(QUERY_TYPE_SESSION_DATA);

        XMLWriter.writeTextElement("ID", query.value("ID").toString());
        XMLWriter.writeTextElement("QueryID", queryId);
        XMLWriter.writeTextElement("Count", query.value("Count").toString());

        XMLWriter.writeEndElement(); //SessionReport

        executedQueryIDList.push_back(QString("'%1'").arg(queryId));
    }

    XMLWriter.writeEndElement();  //SessionReports

    updateSentToDateTime(AZSCode, "QueriesSessionsData",  executedQueryIDList);
}

void TTopaz::markCompliteQuerySQL(const TDocsInfoList &docs, const QString& AZSCode)
{
    //отмечаем все выполненные запросы
    QStringList executedQueryIDList;
    for (const auto& doc: docs)
    {
        if (doc.type == QUERY_TYPE_QUERY)
        {
           executedQueryIDList.push_back(QString("'%1'").arg(doc.queryID));
        }
    }

    if (!executedQueryIDList.isEmpty())
    {
        const QString queryText =
                QString("UPDATE [QueriesToTopaz] "
                        "SET [LoadFromDateTime] = CAST('%1' AS DATETIME2) "
                        "WHERE [QueryID] IN (%2) AND [AZSCode] = '%3'")
                    .arg(QDateTime::currentDateTime().toString(DATETIME_FORMAT))
                    .arg(executedQueryIDList.join(','))
                    .arg(AZSCode);

        writeDebugLogFile(QString("QUERY TO %1>").arg(_db.connectionName()), queryText);

        QSqlQuery query(_db);
        if (!query.exec(queryText))
        {
            errorDBQuery(_db, query);
        }
    }
}

void TTopaz::markCompliteQuerySessionReports(const TDocsInfoList &docs, const QString& AZSCode)
{
    //отмечаем все выполненные запросы
    QStringList executedQueryIDList;
    for (const auto& doc: docs)
    {
        if (doc.type == QUERY_TYPE_SESSION_REPORT)
        {
           executedQueryIDList.push_back(QString("'%1'").arg(doc.queryID));
        }
    }

    if (!executedQueryIDList.isEmpty())
    {
        const QString queryText =
                QString("UPDATE [QueriesSessionReports] "
                        "SET [LoadFromDateTime] = CAST('%1' AS DATETIME2) "
                        "WHERE [QueryID] IN (%2) AND [AZSCode] = '%3'")
                    .arg(QDateTime::currentDateTime().toString(DATETIME_FORMAT))
                    .arg(executedQueryIDList.join(','))
                    .arg(AZSCode);

        writeDebugLogFile(QString("QUERY TO %1>").arg(_db.connectionName()), queryText);

        QSqlQuery query(_db);
        if (!query.exec(queryText))
        {
            errorDBQuery(_db, query);
        }
    }
}

void TTopaz::markCompliteQuerySessionsData(const TDocsInfoList &docs, const QString& AZSCode)
{
    //отмечаем все выполненные запросы
    QStringList executedQueryIDList;
    for (const auto& doc: docs)
    {
        if (doc.type == QUERY_TYPE_SESSION_DATA)
        {
           executedQueryIDList.push_back(QString("'%1'").arg(doc.queryID));
        }
    }

    if (!executedQueryIDList.isEmpty())
    {
        const QString queryText =
                QString("UPDATE [QueriesSessionsData] "
                        "SET [LoadFromDateTime] = CAST('%1' AS DATETIME2) "
                        "WHERE [QueryID] IN (%2) AND [AZSCode] = '%3'")
                    .arg(QDateTime::currentDateTime().toString(DATETIME_FORMAT))
                    .arg(executedQueryIDList.join(','))
                    .arg(AZSCode);

        writeDebugLogFile(QString("QUERY TO %1>").arg(_db.connectionName()), queryText);

        QSqlQuery query(_db);
        if (!query.exec(queryText))
        {
            errorDBQuery(_db, query);
        }
    }
}

TTopaz::TDocsInfoList TTopaz::parseDocuments(QXmlStreamReader &XMLReader)
{
    TDocsInfoList result;

    while ((XMLReader.readNext() != QXmlStreamReader::EndElement) && !XMLReader.atEnd() && !XMLReader.hasError())
    {
        if (XMLReader.name().toString().isEmpty())
        {
            continue;
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
                    doc.dateTime = QDateTime::fromString(XMLReader.readElementText(), DATETIME_FORMAT);
                }
                else if (XMLReader.name().toString()  == "DocumentNumber")
                {
                    bool ok = true;
                    doc.number = XMLReader.readElementText().toInt(&ok);
                    if (!ok)
                    {
                        throw std::runtime_error(QString("Incorrect value tag (Root/Documents/Document/%1). Value: %2. Value must be number")
                                .arg(XMLReader.name().toString())
                                .arg(XMLReader.readElementText()).toStdString());
                    }
                }
                else if (XMLReader.name().toString()  == "Smena")
                {
                    bool ok = true;
                    doc.smena = XMLReader.readElementText().toInt(&ok);
                    if (!ok)
                    {
                        throw std::runtime_error(QString("Incorrect value tag (Root/Documents/Document/%1). Value: %2. Value must be number")
                                .arg(XMLReader.name().toString())
                                .arg(XMLReader.readElementText()).toStdString());
                    }
                }
                else if (XMLReader.name().toString()  == "QueryID")
                {
                    doc.queryID = XMLReader.readElementText();
                    if (doc.queryID.length() > 25)
                    {
                         throw std::runtime_error(QString("Incorrect value tag (Root/Documents/Document/%1). Value: %2. Value must be string shorter than 25 chars")
                                .arg(XMLReader.name().toString())
                                .arg(XMLReader.readElementText()).toStdString());
                    }
                }
                else if (XMLReader.name().toString()  == "Creater")
                {
                    doc.creater = XMLReader.readElementText();
                }
                else if (XMLReader.name().toString()  == "Body")
                {
                    doc.body = XMLReader.readElementText();
                }
                else
                {
                    throw std::runtime_error(QString("Undefine tag in XML (Root/Documents/Document/%1)")
                                             .arg(XMLReader.name().toString()).toStdString());
                }
            }

            if (!doc.type.isEmpty() && !doc.dateTime.isNull())
            {
                result.push_back(doc);
            }
            else
            {
                throw std::runtime_error(QString("Insufficient parameters tag in XML (Root/Documents/Document)").toStdString());
            }
        }
        else
        {
            throw std::runtime_error(QString("Undefine tag in XML (Root/Documents/%1)")
                    .arg(XMLReader.name().toString()).toStdString());
        }
    }

    return result;
}
