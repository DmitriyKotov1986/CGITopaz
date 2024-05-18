//QT
#include <QSettings>
#include <QFileInfo>
#include <QDebug>
#include <QDir>

#include "tconfig.h"

using namespace CGITopaz;
using namespace Common;

static TConfig* config_ptr = nullptr;

TConfig *TConfig::config(const QString &configFileName)
{
    if (config_ptr == nullptr)
    {
        config_ptr = new TConfig(configFileName);
    }

    return config_ptr;
}

void TConfig::deleteConfig()
{
    Q_CHECK_PTR(config_ptr);

    delete config_ptr;

    config_ptr = nullptr;
}

TConfig::TConfig(const QString& configFileName) :
    _configFileName(configFileName)
{
    if (_configFileName.isEmpty())
    {
        _errorString = "Configuration file name cannot be empty";
        return;
    }
    if (!QFileInfo(_configFileName).exists())
    {
        _errorString = "Configuration file not exist. File name: " + _configFileName;
        return;
    }

    qDebug() << QString("%1 %2").arg(QTime::currentTime().toString(SIMPLY_TIME_FORMAT)).arg("Reading configuration from " +  _configFileName);

    QSettings ini(_configFileName, QSettings::IniFormat);

    //Database
    ini.beginGroup("DATABASE");

    _db_ConnectionInfo.db_Driver = ini.value("Driver", "QODBC").toString();
    _db_ConnectionInfo.db_DBName = ini.value("DataBase", "SystemMonitorDB").toString();
    _db_ConnectionInfo.db_UserName = ini.value("UID", "").toString();
    _db_ConnectionInfo.db_Password = ini.value("PWD", "").toString();
    _db_ConnectionInfo.db_ConnectOptions = ini.value("ConnectionOptions", "").toString();
    _db_ConnectionInfo.db_Port = ini.value("Port", "").toUInt();
    _db_ConnectionInfo.db_Host = ini.value("Host", "localhost").toString();

    ini.endGroup();
}

