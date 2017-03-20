package me.j360.idgen.client;

import me.j360.idgen.IdGenService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Pool that use local memory to cache IDs.
 * It will automatically fetch new IDs from global ID generator through dubbo service "com.pajk.idgen.IDGenService"
 *
 * CAUTION:
 * This id pool will PERMANENTLY LOSE IDs cached in memory when JVM restarts.
 *
 * @author Haomin Liu
 */
public class MemIDPool implements IDPool {

    private static final Logger Log = LoggerFactory.getLogger(MemIDPool.class);

    private IdGenService globalIdGenerator;

    private ConcurrentLinkedQueue<String> freshIds;

    private ConcurrentHashSet<String> lentIds;

    // numbers of IDs to be fetched from idGenerator one time.
    private int allocCount = 20;

    // the pool will alloc new ids from global IdGenerator if size of the freshIds less than this value
    private int poolLowerBound = 10;

    // should log error if lent ids is above this number
    private int lentPoolUpperBound = 100000;

    private String idConfigDomain;

    private String idConfigKey;

    /**
     * @param configDomain coordinates needed by global idGenerator service
     * @param configKey coordinates needed by global idGenerator service
     */
    public MemIDPool(String configDomain, String configKey, IdGenService generator){
        globalIdGenerator = generator;
        freshIds = new ConcurrentLinkedQueue<String>();
        lentIds = new ConcurrentHashSet<String>();

        if((configDomain == null)||(configKey == null)){
            throw new RuntimeException("Neither configDomain nor configKey can be null!");
        }

        this.idConfigDomain = configDomain;
        this.idConfigKey = configKey;
    }

    @Override
    public String borrow() {
        if(freshIds.size() <= poolLowerBound){
            synchronized (this){
                // in case of race condition, double check to prevent unnecessary invocation to idGenerator
                if(freshIds.size() <= poolLowerBound){
                    String rawIds = globalIdGenerator.getNextId(idConfigDomain, idConfigKey, allocCount);
                    List<String> newIds = Arrays.asList(rawIds.split(","));

                    Log.debug("fresh ID pool size({}) is running low, allocate new {} IDs: {}", freshIds.size(), allocCount, rawIds);

                    // TODO we should have a dedicated dev-config in remote IDGenService
                    if("user-service-dev".equals(idConfigDomain)){
                        // in develop env, all IDs generated will shift 1 0000 0000, in order to separate daily and ci invokes
                        List<String> devNewIds = new ArrayList<String>();
                        for(String id : newIds){
                            devNewIds.add(String.format("%d", (Long.valueOf(id) + 100000000L)));
                        }
                        newIds = devNewIds;
                    }
                    freshIds.addAll(newIds);
                }
            }
        }

        // put this id to lent pool
        String id = null;
        synchronized (this){
            id = freshIds.poll();
            lentIds.add(id);
        }

        int size = lentIds.size();
        if(size >= lentPoolUpperBound){
            Log.warn("ID pool have lent out over {} ids! Discard these ids!", size);
            lentIds.clear();
        }

        return id;
    }

    @Override
    public void giveback(String id) {
        if(id == null){
            throw new IllegalArgumentException("id can not be null!");
        }

        if(lentIds.contains(id)){
            synchronized (this){
                if(lentIds.contains(id)){
                    lentIds.remove(id);
                    freshIds.add(id);
                }
            }
        }
    }

    @Override
    public void consume(String id) {
        if(id == null){
            throw new IllegalArgumentException("id can not be null!");
        }

        if(lentIds.contains(id)){
            synchronized (this){
                if(lentIds.contains(id)){
                    lentIds.remove(id);
                }
            }
        }
    }

    public int getLentPoolSize(){
        return lentIds.size();
    }

    public int getFreshPoolSize(){
        return freshIds.size();
    }

    // getter and setters
    public int getAllocCount() {
        return allocCount;
    }

    public void setAllocCount(int allocCount) {
        this.allocCount = allocCount;
    }

    public int getPoolLowerBound() {
        return poolLowerBound;
    }

    public void setPoolLowerBound(int poolLowerBound) {
        this.poolLowerBound = poolLowerBound;
    }

    public int getLentPoolUpperBound() {
        return lentPoolUpperBound;
    }

    public void setLentPoolUpperBound(int lentPoolUpperBound) {
        this.lentPoolUpperBound = lentPoolUpperBound;
    }

    public IDGenService getGlobalIdGenerator() {
        return globalIdGenerator;
    }

    public void setGlobalIdGenerator(IDGenService idGenerator) {
        this.globalIdGenerator = idGenerator;
    }

}

