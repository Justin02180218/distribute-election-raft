package com.justin.distribute.election.raft.log;

import com.justin.distribute.election.raft.common.PropertiesUtil;
import com.justin.net.remoting.protocol.JSONSerializable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * WX: coding到灯火阑珊
 * @author Justin
 */
public class Log {
    private static final Logger logger = LogManager.getLogger(Log.class.getSimpleName());

    private final static byte[] LAST_INDEX_KEY = "last_index_key".getBytes(Charset.forName("UTF-8"));
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private static RocksDB logDb;

    static {
        RocksDB.loadLibrary();
    }

    private Log() {
        try {
            File file = new File(PropertiesUtil.getLogDir());
            if (!file.exists()) {
                file.mkdirs();
            }

            Options options = new Options();
            options.setCreateIfMissing(true);
            logDb = RocksDB.open(options, PropertiesUtil.getLogDir());
        }catch (Exception e) {
            logger.error(e);
        }
    }

    private static class LogSingle {
        private final static Log INSTANCE = new Log();
    }

    public static Log getInstance() {
        return LogSingle.INSTANCE;
    }

    public void write(final LogEntry logEntry) {
        try {
            if (lock.writeLock().tryLock(3000, TimeUnit.MILLISECONDS)) {
                Long nextIndex = getLastIndex() + 1;
                logEntry.setIndex(nextIndex);
                logDb.put(nextIndex.toString().getBytes("UTF-8"), JSONSerializable.encode(logEntry));
                updateLastIndex(nextIndex);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            lock.writeLock().unlock();
        }
    }

    public LogEntry read(Long index) {
        try {
            if (lock.readLock().tryLock(3000, TimeUnit.MILLISECONDS)) {
                byte[] value = logDb.get(index.toString().getBytes("UTF-8"));
                if (value != null) {
                    return JSONSerializable.decode(value, LogEntry.class);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            lock.readLock().unlock();
        }
        return null;
    }

    public void removeFromIndex(Long index) {
        try {
            if (lock.writeLock().tryLock(3000, TimeUnit.MILLISECONDS)) {
                byte[] startIndex = index.toString().getBytes(Charset.forName("UTF-8"));
                byte[] lastIndex = getLastIndex().toString().getBytes(Charset.forName("UTF-8"));
                logDb.deleteRange(startIndex, lastIndex);
                updateLastIndex(index - 1);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            lock.writeLock().unlock();
        }
    }

    public LogEntry getLastLogEntry() {
        return read(getLastIndex());
    }

    public Long getLastIndex() {
        try {
            byte[] lastIndex = logDb.get(LAST_INDEX_KEY);
            if (lastIndex != null) {
                return Long.valueOf(new String(lastIndex, Charset.forName("UTF-8")));
            }
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
        return -1l;
    }

    private void updateLastIndex(final Long index) {
        try {
            logDb.put(LAST_INDEX_KEY, index.toString().getBytes(Charset.forName("UTF-8")));
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    public void close() {
        if (logDb != null) {
            logDb.close();
        }
    }
}
