using MySql.Data.MySqlClient;
using NBsoft.Logs.Interfaces;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;

namespace NBsoft.Logs.Sql
{
    public class MySqlLogger : ILogger
    {
        private static readonly System.Threading.SemaphoreSlim semapore = new System.Threading.SemaphoreSlim(1);

        private readonly string _connString;
        private readonly string _logTable;
        private readonly int _cacheMaxEntries;
        private readonly List<ILogItem> _logCache;
        private readonly System.Threading.Timer writeTimer;
                
        private DateTime lastWrite;
        private bool isTerminating;

        public MySqlLogger(string connString, string logTable, int cacheMaxEntries = 16)
        {
            isTerminating = false;
            _connString = connString;
            _logTable = logTable;
            _cacheMaxEntries = cacheMaxEntries;
            _logCache = new List<ILogItem>();

            lastWrite = DateTime.MinValue;
            writeTimer = new System.Threading.Timer(WriteTimerCallback);

            CheckDatabase();            
            writeTimer.Change(30 * 1000, -1);
        }
        ~MySqlLogger()
        {
            Terminate();
        }

        private void CheckDatabase()
        {
            DbConnection testConn = new MySqlConnection(_connString);
            testConn.Open();
            bool LogTableExists = false;
            try
            {   
                try
                {
                    // Check if log table exists
                    DbCommand cmd = testConn.CreateCommand();
                    cmd.CommandText = string.Format("SELECT 1 FROM {0} LIMIT 1;", _logTable);
                    cmd.ExecuteScalar();
                    LogTableExists = true;
                }
                catch { LogTableExists = false; }
                if (!LogTableExists)
                {
                    string sql = string.Format(
                        "CREATE TABLE {0} (" +
                        " Id                  bigint                    NOT NULL AUTO_INCREMENT," +
                        " DateTime            datetime                  NOT NULL, " +
                        " Level               nvarchar(16)	            NOT NULL, " +
                        " Component           text                      NULL, " +
                        " Process             text                      NULL, " +
                        " Context             text                      NULL, " +
                        " Type                text                      NULL, " +
                        " Stack               text                      NULL, " +
                        " Msg                 text                      NULL" +                        
                        ");", _logTable);

                    DbCommand createCommand = testConn.CreateCommand();
                    createCommand.CommandText = sql;
                    createCommand.ExecuteNonQuery();
                }
            }
            finally
            {
                testConn.Dispose();
            }
            if (!LogTableExists)
            {
                WriteInfo(nameof(MySqlLogger), nameof(CheckDatabase), _logTable, "MySqlLogger table created");
                SaveCache();
            }

        }
        private MySqlConnection CreateConnection()
        {
            var conn = new MySqlConnection(_connString);
            conn.Open();
            return conn;

        }
        private void WriteTimerCallback(object status)
        {
            writeTimer.Change(-1, -1);

            if (lastWrite.AddSeconds(60) <= DateTime.UtcNow)
            {
                lastWrite = DateTime.UtcNow;
                if (_logCache.Count >= 1)
                    SaveCache();
            }

            writeTimer.Change(30 * 1000, -1);
        }
        private void Terminate()
        {
            if (isTerminating)
                return;
            isTerminating = true;
            writeTimer.Change(-1, -1);
            writeTimer.Dispose();

            SaveCache();
        }

        private void AddToCache(ILogItem item)
        {
            semapore.Wait();
            try { _logCache.Add(item); }
            finally { semapore.Release(); }
            if (_logCache.Count >= _cacheMaxEntries)
                SaveCache();
        }
        private int SaveCache()
        {
            if (_logCache.Count < 1)
                return 0;

            ILogItem[] tempCache;
            semapore.Wait();
            try
            {
                tempCache = _logCache.ToArray();
                _logCache.Clear();
            }
            finally { semapore.Release(); }
            
            var result = 0;
            using (var conn = CreateConnection())
            {
                var transaction = conn.BeginTransaction();
                foreach (var logItem in tempCache)
                {
                    using (DbCommand cmd = conn.CreateCommand())
                    {
                        cmd.Transaction = transaction;
                        cmd.CommandText = string.Format("INSERT INTO {0}  (DateTime,Level,Component,Process,Context,Type,Stack,Msg)" +
                            " VALUES " +
                            "(@DateTime,@Level,@Component,@Process,@Context,@Type,@Stack,@Msg)"
                            , _logTable);

                        cmd.Parameters.Add(new MySqlParameter("@DateTime", logItem.DateTime));
                        cmd.Parameters.Add(new MySqlParameter("@Level", logItem.Level));
                        cmd.Parameters.Add(new MySqlParameter("@Component", (object)logItem.Component ?? DBNull.Value));
                        cmd.Parameters.Add(new MySqlParameter("@Process", (object)logItem.Process ?? DBNull.Value));
                        cmd.Parameters.Add(new MySqlParameter("@Context", (object)logItem.Context ?? DBNull.Value));
                        cmd.Parameters.Add(new MySqlParameter("@Type", (object)logItem.Type ?? DBNull.Value));
                        cmd.Parameters.Add(new MySqlParameter("@Stack", (object)logItem.Stack ?? DBNull.Value));
                        cmd.Parameters.Add(new MySqlParameter("@Msg", (object)logItem.Message ?? DBNull.Value));
                        try { result = cmd.ExecuteNonQuery(); }
                        catch (Exception ex01)
                        {
                            Console.WriteLine(ex01.Message);
                            throw;
                        }
                    }
                }
                transaction.Commit();                
            }
            return result;
        }
        

        private Task WriteLogAsync(LogType level, string component, string process, string context, string message, string stack, string type, DateTime? dateTime = default(DateTime?))
        {
            return WriteLogAsync(new Models.LogItem()
            {
                Level = level,
                Component = component,
                Process = process,
                Context = context,
                Message = message,
                Stack = stack,
                Type = type,
                DateTime = dateTime ?? DateTime.UtcNow
            });
        }
        private void WriteLog(LogType level, string component, string process, string context, string message, string stack, string type, DateTime? dateTime = default(DateTime?))
        {
            WriteLog(new Models.LogItem()
            {
                Level = level,
                Component = component,
                Process = process,
                Context = context,
                Message = message,
                Stack = stack,
                Type = type,
                DateTime = dateTime ?? DateTime.UtcNow
            });
        }

        public Task WriteLogAsync(ILogItem item)
        {
            AddToCache(item);
            return Task.FromResult(0);
        }
        public Task WriteInfoAsync(string component, string process, string context, string message, DateTime? dateTime = default(DateTime?))
        {
            return WriteLogAsync(LogType.Info, component, process, context, message, null, null, dateTime);
        }
        public Task WriteWarningAsync(string component, string process, string context, string message, DateTime? dateTime = default(DateTime?))
        {
            return WriteLogAsync(LogType.Warning, component, process, context, message, null, null, dateTime);
        }
        public Task WriteErrorAsync(string component, string process, string context, string message, Exception exception, DateTime? dateTime = default(DateTime?))
        {
            return WriteLogAsync(LogType.Error, component, process, context, $"{message} - {exception.Message}", exception.GetBaseException().StackTrace, exception.GetBaseException().GetType().ToString(), dateTime);
        }
        public Task WriteFatalErrorAsync(string component, string process, string context, string message, Exception exception, DateTime? dateTime = default(DateTime?))
        {
            return WriteLogAsync(LogType.FatalError, component, process, context, $"{message} - {exception.Message}", exception.GetBaseException().StackTrace, exception.GetBaseException().GetType().ToString(), dateTime);
        }

        public void WriteLog(ILogItem item)
        {
            AddToCache(item);
        }
        public void WriteInfo(string component, string process, string context, string message, DateTime? dateTime = null)
        {
            WriteLog(LogType.Info, component, process, context, message, null, null, dateTime);
        }
        public void WriteWarning(string component, string process, string context, string message, DateTime? dateTime = null)
        {
            WriteLog(LogType.Warning, component, process, context, message, null, null, dateTime);
        }
        public void WriteError(string component, string process, string context, string message, Exception exception, DateTime? dateTime = null)
        {
            WriteLog(LogType.Error, component, process, context, $"{message} - {exception.Message}", exception.GetBaseException().StackTrace, exception.GetBaseException().GetType().ToString(), dateTime);
        }
        public void WriteFatalError(string component, string process, string context, string message, Exception exception, DateTime? dateTime = null)
        {
            WriteLog(LogType.FatalError, component, process, context, $"{message} - {exception.Message}", exception.GetBaseException().StackTrace, exception.GetBaseException().GetType().ToString(), dateTime);
        }

        public void Dispose()
        {
            Terminate();
        }

      
    }
}
