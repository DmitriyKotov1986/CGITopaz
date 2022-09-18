#ifndef TSYNC_H
#define TSYNC_H

#include <QObject>
#include <QtSql/QSqlDatabase>
#include <QDateTime>
#include <QList>

#include "tconfig.h"

namespace CGITopaz {

class TTopaz
{
public:
    explicit TTopaz(CGITopaz::TConfig* cfg);
    ~TTopaz();

public:
    int run(const QString& XMLText);
    QString errorString() const { return _errorString; }

private:
    typedef struct {
        QString Body;
        int number;
        int smena;
        QString type;
        QDateTime dateTime;
        QString creater;
    } TDocInfo;

    typedef QList<TDocInfo> TDocsInfoList;

    QSqlDatabase _db;
    QString _errorString;
};

} //namespace CGITopaz

#endif // TSYNC_H
