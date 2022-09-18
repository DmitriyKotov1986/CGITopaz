#include "ttopaz.h"

//QT
#include <QSqlError>
#include <QSqlQuery>
#include <QXmlStreamReader>

//My
#include "common.h"

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
    if (_db.isOpen()) {
        _db.close();
    }
}

int TTopaz::run(const QString& XMLText)
{
    writeDebugLogFile("REQUEST>", XMLText);

    if (XMLText.isEmpty()) {
        return EXIT_CODE::XML_EMPTY;
    }

    if (!_db.open()) {
        _errorString = "Cannot connet to DB. Error: " + _db.lastError().text();
        return EXIT_CODE::SQL_NOT_OPEN_DB;
    }

    QSqlQuery query(_db);
    query.setForwardOnly(true);
    _db.transaction();

    QTextStream textStream(stdout);

    //парсим XML
    QXmlStreamReader XMLReader(XMLText);
    QString AZSCode = "n/a";
    QString clientVersion = "n/a";
    QString protocolVersion = "n/a";
    TDocsInfoList docs;

    while ((!XMLReader.atEnd()) && (!XMLReader.hasError())) {
        QXmlStreamReader::TokenType token = XMLReader.readNext();
        if (token == QXmlStreamReader::StartDocument) continue;
        else if (token == QXmlStreamReader::EndDocument) break;
        else if (token == QXmlStreamReader::StartElement) {
            //qDebug() << XMLReader.name().toString();
            if (XMLReader.name().toString()  == "Root") {
                while ((XMLReader.readNext() != QXmlStreamReader::EndElement) || XMLReader.atEnd() || XMLReader.hasError()) {
                    //qDebug() << "Root/" << XMLReader.name().toString();
                    if (XMLReader.name().toString().isEmpty()) {
                        continue;
                    }
                    if (XMLReader.name().toString()  == "AZSCode") {
                        AZSCode = XMLReader.readElementText();
                    }
                    else if (XMLReader.name().toString()  == "ClientVersion") {
                        clientVersion = XMLReader.readElementText();
                    }
                    else if (XMLReader.name().toString()  == "ProtocolVersion") {
                        protocolVersion = XMLReader.readElementText();
                    }
                    else if (XMLReader.name().toString()  == "Document") {
                        TDocInfo doc;
                        while ((XMLReader.readNext() != QXmlStreamReader::EndElement) || XMLReader.atEnd() || XMLReader.hasError()) {
                            //qDebug() << "Root/LevelGauge/" << XMLReader.name().toString();
                            if (XMLReader.name().toString().isEmpty()) {
                                continue;
                            }
                            else if (XMLReader.name().toString()  == "DocumentType") {
                                doc.type = XMLReader.readElementText();
                            }
                            else if (XMLReader.name().toString()  == "DateTime") {
                                doc.dateTime = QDateTime::fromString(XMLReader.readElementText(), "yyyy-MM-dd hh:mm:ss.zzz");
                            }
                            else if (XMLReader.name().toString()  == "DocumentNumber") {
                                doc.number = XMLReader.readElementText().toInt();
                            }
                            else if (XMLReader.name().toString()  == "Smena") {
                                doc.smena = XMLReader.readElementText().toInt();
                            }
                            else if (XMLReader.name().toString()  == "Creater") {
                                doc.creater = XMLReader.readElementText();
                            }
                            else if (XMLReader.name().toString()  == "Body") {
                                doc.Body = XMLReader.readElementText();
                            }

                            else {
                                _errorString = "Undefine tag in XML (LevelGauge/" + XMLReader.name().toString() + ")";
                                textStream << _errorString; //выводим сообщение об ошибке в cin для отправки клиенту
                                return EXIT_CODE::XML_UNDEFINE_TOCKEN;
                            }
                        }
                        docs.push_back(doc);
                    }
                    else {
                        _errorString = "Undefine tag in XML (Root/" + XMLReader.name().toString() + ")";
                        textStream << _errorString; //выводим сообщение об ошибке в cin для отправки клиенту
                        return EXIT_CODE::XML_UNDEFINE_TOCKEN;
                    }
                }
            }
        }
        else {
            _errorString = "Undefine token in XML";
            textStream << _errorString; //выводим сообщение об ошибке в cin для отправки клиенту
            return EXIT_CODE::XML_UNDEFINE_TOCKEN;
        }
    }

    if (XMLReader.hasError()) { //неудалось распарсить пришедшую XML
        _errorString = "Cannot parse XML query. Message: " + XMLReader.errorString();
        textStream << _errorString; //выводим сообщение об ошибке в cin для отправки клиенту
        return EXIT_CODE::XML_PARSE_ERR;
    }

    if (!docs.isEmpty()) {
        QString docsValues;
        for (const auto& docItem: docs) {
            if (!docsValues.isEmpty()) {
                docsValues += ", ";
            }
            docsValues += QString("'%1', CAST('%2' AS datetime2), '%3', %4, %5, '%6', '%7'").
                            arg(AZSCode).arg(docItem.dateTime.toString("yyyy-MM-dd hh:mm:ss.zzz")).
                            arg(docItem.type).arg(docItem.number).arg(docItem.smena).arg(docItem.creater).arg(docItem.Body);
        }

        QString queryText = QString("INSERT INTO [TopazDocuments] ([AZSCode], [DateTime], [DocumentType], [DocumentNumber], [Smena], [Creater], [Body]) "
                                    "VALUES (%1) ").arg(docsValues);

        writeDebugLogFile("QUERY>", queryText);

        if (!query.exec(queryText)) {
            _db.rollback();
            _errorString = "Cannot execute query. Error: " + query.lastError().text() + " Query: " + queryText;
            return EXIT_CODE::SQL_EXECUTE_QUERY_ERR;
        }

        if (!_db.commit()) {
            _db.rollback();
            _errorString = "Cannot commit transaction. Error: " + _db.lastError().text();
            return EXIT_CODE::SQL_COMMIT_ERR;
        }
    }

    //отправляем ответ
    textStream << "OK";

    writeDebugLogFile("ANSWER>", "OK");

    return 0;
}
