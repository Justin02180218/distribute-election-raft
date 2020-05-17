package com.justin.distribute.election.raft.log;

import com.justin.distribute.election.raft.common.PropertiesUtil;
import com.justin.net.remoting.protocol.JSONSerializable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

import java.io.File;
import java.nio.charset.Charset;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * WX: coding到灯火阑珊
 * @author Justin
 */
public class Snapshot {
    private static final Logger logger = LogManager.getLogger(Snapshot.class.getSimpleName());

    private static RocksDB machineDd;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    static {
        RocksDB.loadLibrary();
    }

    private Snapshot() {
        try {
            File file = new File(PropertiesUtil.getSnapshoteDir());
            if (!file.exists()) {
                file.mkdirs();
            }

            Options options = new Options();
            options.setCreateIfMissing(true);

            machineDd = RocksDB.open(options, PropertiesUtil.getSnapshoteDir());
        } catch (Exception e) {
            logger.error(e);
        }
    }

    private static class StateMachineSingle {
        private static final Snapshot INSTANCE = new Snapshot();
    }

    public static Snapshot getInstance() {
        return StateMachineSingle.INSTANCE;
    }

    public boolean apply(final LogEntry logEntry) {
        try {
            if (lock.writeLock().tryLock(3000, TimeUnit.MILLISECONDS)) {
                if (logEntry.getKv() == null) {
                    throw new IllegalArgumentException("LogEntry's KV can't null!");
                }
                String key = logEntry.getKv().getObject1();
                machineDd.put(key.getBytes(Charset.forName("UTF-8")), JSONSerializable.encode(logEntry));
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            lock.writeLock().unlock();
        }
        return false;
    }

    public LogEntry get(final String key) {
        try {
            if (lock.readLock().tryLock(3000, TimeUnit.MILLISECONDS)) {
                byte[] value = machineDd.get(key.getBytes(Charset.forName("UTF-8")));
                if (value == null) {
                    return null;
                }

                return JSONSerializable.decode(value, LogEntry.class);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            lock.readLock().unlock();
        }
        return null;
    }

    public void close() {
        if (machineDd != null) {
            machineDd.close();
        }
    }
}
