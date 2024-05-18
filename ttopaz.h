#ifndef TSYNC_H
#define TSYNC_H

//SQL
#include <QObject>
#include <QtSql/QSqlDatabase>
#include <QDateTime>
#include <QList>
#include <QXmlStreamWriter>

namespace CGITopaz
{

class TTopaz
{
public:
    TTopaz() = default;
    ~TTopaz();

public:
    int run(const QString& XMLText);
    QString errorString() const { return _errorString; }

private:
    struct TDocInfo
    {
        QString queryID;
        QString body;
        int number;
        int smena;
        QString type;
        QDateTime dateTime;
        QString creater;
    };

    using TDocsInfoList = QList<TDocInfo>;

private:
    void connectToDB();

    void saveDocs(const QString& AZSCode, const TDocsInfoList& docs);
    void updateSentToDateTime(const QString& AZSCode, const QString& tableName, const QStringList& executedQueryIDList);

    TDocsInfoList parseDocuments(QXmlStreamReader& XMLReader);

    void addQuerySQL(QXmlStreamWriter& XMLWriter, const QString& AZSCode, quint64 lastQueryID);
    void addQuerySessionReports(QXmlStreamWriter& XMLWriter, const QString& AZSCode,quint64 lastQueryID);
    void addQuerySessionsData(QXmlStreamWriter& XMLWriter, const QString& AZSCode, quint64 lastQueryID);

    void markCompliteQuerySQL(const TDocsInfoList& docs, const QString& AZSCode);
    void markCompliteQuerySessionReports(const TDocsInfoList &docs, const QString& AZSCode);
    void markCompliteQuerySessionsData(const TDocsInfoList &docs, const QString& AZSCode);

private:
    QSqlDatabase _db;
    QString _errorString;
};

} //namespace CGITopaz

#endif // TSYNC_H
