/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2013 Couchbase, Inc
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#ifndef SRC_EP_ENGINE_H_
#define SRC_EP_ENGINE_H_ 1

#include "config.h"

#include "arenamanager.h"
#include "kv_bucket.h"
#include "storeddockey.h"
#include "tapconnection.h"
#include "taskable.h"
#include "vbucket.h"

#include <memcached/engine.h>
#include <platform/processclock.h>

#include <string>

class StoredValue;
class DcpConnMap;
class DcpFlowControlManager;
class Producer;
class TapConnMap;
class ReplicationThrottle;
class VBucketCountVisitor;

extern "C" {
    EXPORT_FUNCTION
    ENGINE_ERROR_CODE create_instance(uint64_t interface,
                                      GET_SERVER_API get_server_api,
                                      ENGINE_HANDLE **handle);

    EXPORT_FUNCTION
    void destroy_engine(void);

    void EvpNotifyPendingConns(void*arg);
}

/* We're using notify_io_complete from ptr_fun, but that func
 * got a "C" linkage that ptr_fun doesn't like... just
 * cast it away with this typedef ;)
 */
typedef void (*NOTIFY_IO_COMPLETE_T)(const void *cookie,
                                     ENGINE_ERROR_CODE status);

// Forward decl
class EventuallyPersistentEngine;
class TapConnMap;

/**
    To allow Engines to run tasks.
**/
class EpEngineTaskable : public Taskable {
public:
    EpEngineTaskable(EventuallyPersistentEngine* e) : myEngine(e) {

    }

    const std::string& getName() const;

    task_gid_t getGID() const;

    bucket_priority_t getWorkloadPriority() const;

    void setWorkloadPriority(bucket_priority_t prio);

    WorkLoadPolicy& getWorkLoadPolicy(void);

    void logQTime(TaskId id, const ProcessClock::duration enqTime);

    void logRunTime(TaskId id, const ProcessClock::duration runTime);

private:
    EventuallyPersistentEngine* myEngine;
};

/**
 * A container class holding VBucketCountVisitors to aggregate stats for
 * different vbucket states.
 */
class VBucketCountAggregator : public VBucketVisitor  {
public:
    void visitBucket(RCPtr<VBucket> &vb) override;

    void addVisitor(VBucketCountVisitor* visitor);
private:
    std::map<vbucket_state_t, VBucketCountVisitor*> visitorMap;
};

/**
 * memcached engine interface to the KVBucket.
 */
class EventuallyPersistentEngine : public ENGINE_HANDLE_V1 {
    friend class LookupCallback;
public:
    ENGINE_ERROR_CODE initialize(const char* config);
    void destroy(bool force);

    ENGINE_ERROR_CODE itemAllocate(item** itm,
                                   const DocKey& key,
                                   const size_t nbytes,
                                   const size_t priv_nbytes,
                                   const int flags,
                                   const rel_time_t exptime,
                                   uint8_t datatype,
                                   uint16_t vbucket);
    /**
     * Delete a given key and value from the engine.
     *
     * @param cookie The cookie representing the connection
     * @param key The key that needs to be deleted from the engine
     * @param cas CAS value of the mutation that needs to be returned
     *            back to the client
     * @param vbucket vbucket id to which the deleted key corresponds to
     * @param itm item pointer that contains a value that needs to be
     *            stored along with a delete. A NULL pointer indicates
     *            that no value needs to be stored with the delete.
     * @param item_meta pointer to item meta data that needs to be
     *                  as a result the delete. A NULL pointer indicates
     *                  that no meta data needs to be returned.
     * @param mut_info pointer to the mutation info that resulted from
     *                 the delete.
     *
     * @returns ENGINE_SUCCESS if the delete was successful or
     *          an error code indicating the error
     */
    ENGINE_ERROR_CODE itemDelete(const void* cookie,
                                 const DocKey& key,
                                 uint64_t& cas,
                                 uint16_t vbucket,
                                 Item* itm,
                                 ItemMetaData* item_meta,
                                 mutation_descr_t* mut_info) {
        ENGINE_ERROR_CODE ret = kvBucket->deleteItem(key,
                                                     cas,
                                                     vbucket,
                                                     cookie,
                                                     itm,
                                                     item_meta,
                                                     mut_info);

        if (ret == ENGINE_KEY_ENOENT || ret == ENGINE_NOT_MY_VBUCKET) {
            if (isDegradedMode()) {
                return ENGINE_TMPFAIL;
            }
        } else if (ret == ENGINE_SUCCESS) {
            ++stats.numOpsDelete;
        }
        return ret;
    }


    void itemRelease(const void* cookie, item *itm)
    {
        (void)cookie;
        delete (Item*)itm;
    }

    ENGINE_ERROR_CODE get(const void* cookie,
                          item** itm,
                          const DocKey& key,
                          uint16_t vbucket,
                          get_options_t options)
    {
        BlockTimer timer(&stats.getCmdHisto);
        GetValue gv(kvBucket->get(key, vbucket, cookie, options));
        ENGINE_ERROR_CODE ret = gv.getStatus();

        if (ret == ENGINE_SUCCESS) {
            *itm = gv.getValue();
            if (options & TRACK_STATISTICS) {
                ++stats.numOpsGet;
            }
        } else if (ret == ENGINE_KEY_ENOENT || ret == ENGINE_NOT_MY_VBUCKET) {
            if (isDegradedMode()) {
                return ENGINE_TMPFAIL;
            }
        }

        return ret;
    }

    cb::EngineErrorItemPair get_if(const void* cookie,
                                   const DocKey& key,
                                   uint16_t vbucket,
                                   std::function<bool(
                                       const item_info&)> filter);

    ENGINE_ERROR_CODE get_locked(const void* cookie,
                                 item** itm,
                                 const DocKey& key,
                                 uint16_t vbucket,
                                 uint32_t lock_timeout);


    ENGINE_ERROR_CODE unlock(const void* cookie,
                             const DocKey& key,
                             uint16_t vbucket,
                             uint64_t cas);


    const std::string& getName() const {
        return name;
    }

    ENGINE_ERROR_CODE getStats(const void* cookie,
                               const char* stat_key,
                               int nkey,
                               ADD_STAT add_stat);

    void resetStats() {
        stats.reset();
        if (kvBucket) {
            kvBucket->resetUnderlyingStats();
        }
    }

    ENGINE_ERROR_CODE store(const void *cookie,
                            item* itm,
                            uint64_t *cas,
                            ENGINE_STORE_OPERATION operation);

    ENGINE_ERROR_CODE flush(const void *cookie);

    uint16_t walkTapQueue(const void *cookie, item **itm, void **es,
                          uint16_t *nes, uint8_t *ttl, uint16_t *flags,
                          uint32_t *seqno, uint16_t *vbucket);

    bool createTapQueue(const void *cookie,
                        std::string &client,
                        uint32_t flags,
                        const void *userdata,
                        size_t nuserdata);

    ENGINE_ERROR_CODE tapNotify(const void *cookie,
                                void *engine_specific,
                                uint16_t nengine,
                                uint8_t ttl,
                                uint16_t tap_flags,
                                uint16_t tap_event,
                                uint32_t tap_seqno,
                                const void *key,
                                size_t nkey,
                                uint32_t flags,
                                uint32_t exptime,
                                uint64_t cas,
                                uint8_t datatype,
                                const void *data,
                                size_t ndata,
                                uint16_t vbucket);

    ENGINE_ERROR_CODE dcpOpen(const void* cookie,
                              uint32_t opaque,
                              uint32_t seqno,
                              uint32_t flags,
                              const void *stream_name,
                              uint16_t nname);

    ENGINE_ERROR_CODE dcpAddStream(const void* cookie,
                                   uint32_t opaque,
                                   uint16_t vbucket,
                                   uint32_t flags);

    ENGINE_ERROR_CODE ConnHandlerCheckPoint(TapConsumer *consumer,
                                            uint8_t event,
                                            uint16_t vbucket,
                                            uint64_t checkpointId);

    ENGINE_ERROR_CODE touch(const void* cookie,
                            protocol_binary_request_header *request,
                            ADD_RESPONSE response,
                            DocNamespace docNamespace);

    ENGINE_ERROR_CODE getMeta(const void* cookie,
                              protocol_binary_request_get_meta *request,
                              ADD_RESPONSE response,
                              DocNamespace docNamespace);

    ENGINE_ERROR_CODE setWithMeta(const void* cookie,
                                 protocol_binary_request_set_with_meta *request,
                                 ADD_RESPONSE response,
                                 DocNamespace docNamespace);

    ENGINE_ERROR_CODE deleteWithMeta(const void* cookie,
                              protocol_binary_request_delete_with_meta *request,
                              ADD_RESPONSE response,
                              DocNamespace docNamespace);

    ENGINE_ERROR_CODE returnMeta(const void* cookie,
                                 protocol_binary_request_return_meta *request,
                                 ADD_RESPONSE response,
                                 DocNamespace docNamespace);

    ENGINE_ERROR_CODE setClusterConfig(const void* cookie,
                            protocol_binary_request_set_cluster_config *request,
                            ADD_RESPONSE response);

    ENGINE_ERROR_CODE getClusterConfig(const void* cookie,
                            protocol_binary_request_get_cluster_config *request,
                            ADD_RESPONSE response);

    ENGINE_ERROR_CODE getAllKeys(const void* cookie,
                                protocol_binary_request_get_keys *request,
                                ADD_RESPONSE response,
                                DocNamespace docNamespace);

    /**
     * Visit the objects and add them to the tap/dcp connecitons queue.
     * @todo this code should honor the backfill time!
     */
    void queueBackfill(const VBucketFilter &backfillVBFilter, Producer *tc);

    void setDCPPriority(const void* cookie, CONN_PRIORITY priority) {
        __system_allocation__;
        serverApi->cookie->set_priority(cookie, priority);
    }

    void notifyIOComplete(const void *cookie, ENGINE_ERROR_CODE status) {
        if (cookie == NULL) {
            LOG(EXTENSION_LOG_WARNING, "Tried to signal a NULL cookie!");
        } else {
            BlockTimer bt(&stats.notifyIOHisto);
            __system_allocation__;
            serverApi->cookie->notify_io_complete(cookie, status);
        }
    }

    ENGINE_ERROR_CODE reserveCookie(const void *cookie);
    ENGINE_ERROR_CODE releaseCookie(const void *cookie);

    void storeEngineSpecific(const void *cookie, void *engine_data) {
        __system_allocation__;
        serverApi->cookie->store_engine_specific(cookie, engine_data);
    }

    void *getEngineSpecific(const void *cookie) {
        __system_allocation__;
        void *engine_data = serverApi->cookie->get_engine_specific(cookie);
        return engine_data;
    }

    bool isDatatypeSupported(const void* cookie,
                             protocol_binary_datatype_t datatype) {
        __system_allocation__;
        bool isSupported =
            serverApi->cookie->is_datatype_supported(cookie, datatype);
        return isSupported;
    }

    bool isMutationExtrasSupported(const void *cookie) {
        __system_allocation__;
        bool isSupported = serverApi->cookie->is_mutation_extras_supported(cookie);
        return isSupported;
    }

    bool isXattrSupported(const void* cookie) {
        return isDatatypeSupported(cookie, PROTOCOL_BINARY_DATATYPE_XATTR);
    }

    uint8_t getOpcodeIfEwouldblockSet(const void *cookie) {
        __system_allocation__;
        uint8_t opcode = serverApi->cookie->get_opcode_if_ewouldblock_set(cookie);
        return opcode;
    }

    bool validateSessionCas(const uint64_t cas) {
        __system_allocation__;
        bool ret = serverApi->cookie->validate_session_cas(cas);
        return ret;
    }

    void decrementSessionCtr(void) {
        __system_allocation__;
        serverApi->cookie->decrement_session_ctr();
    }

    void registerEngineCallback(ENGINE_EVENT_TYPE type,
                                EVENT_CALLBACK cb, const void *cb_data);

    template <typename T>
    void notifyIOComplete(T cookies, ENGINE_ERROR_CODE status) {
        __system_allocation__;
        std::for_each(cookies.begin(), cookies.end(),
                      std::bind2nd(std::ptr_fun((NOTIFY_IO_COMPLETE_T)serverApi->cookie->notify_io_complete),
                                   status));
    }

    void handleDisconnect(const void *cookie);
    void handleDeleteBucket(const void *cookie);

    protocol_binary_response_status stopFlusher(const char **msg, size_t *msg_size) {
        (void) msg_size;
        protocol_binary_response_status rv = PROTOCOL_BINARY_RESPONSE_SUCCESS;
        *msg = NULL;
        if (!kvBucket->pauseFlusher()) {
            LOG(EXTENSION_LOG_INFO, "Unable to stop flusher");
            *msg = "Flusher not running.";
            rv = PROTOCOL_BINARY_RESPONSE_EINVAL;
        }
        return rv;
    }

    protocol_binary_response_status startFlusher(const char **msg, size_t *msg_size) {
        (void) msg_size;
        protocol_binary_response_status rv = PROTOCOL_BINARY_RESPONSE_SUCCESS;
        *msg = NULL;
        if (!kvBucket->resumeFlusher()) {
            LOG(EXTENSION_LOG_INFO, "Unable to start flusher");
            *msg = "Flusher not shut down.";
            rv = PROTOCOL_BINARY_RESPONSE_EINVAL;
        }
        return rv;
    }

    ENGINE_ERROR_CODE deleteVBucket(uint16_t vbid, const void* c = NULL) {
        return kvBucket->deleteVBucket(vbid, c);
    }

    ENGINE_ERROR_CODE compactDB(uint16_t vbid,
                                compaction_ctx c,
                                const void *cookie = NULL) {
        return kvBucket->scheduleCompaction(vbid, c, cookie);
    }

    bool resetVBucket(uint16_t vbid) {
        return kvBucket->resetVBucket(vbid);
    }

    void setTapKeepAlive(uint32_t to) {
        configuration.setTapKeepalive((size_t)to);
    }

    void setDeleteAll(bool enabled) {
        deleteAllEnabled = enabled;
    }

    protocol_binary_response_status evictKey(const DocKey& key,
                                             uint16_t vbucket,
                                             const char** msg) {
        return kvBucket->evictKey(key, vbucket, msg);
    }

    ENGINE_ERROR_CODE observe(const void* cookie,
                              protocol_binary_request_header *request,
                              ADD_RESPONSE response,
                              DocNamespace docNamespace);

    ENGINE_ERROR_CODE observe_seqno(const void* cookie,
                                    protocol_binary_request_header *request,
                                    ADD_RESPONSE response);

    RCPtr<VBucket> getVBucket(uint16_t vbucket) {
        return kvBucket->getVBucket(vbucket);
    }

    ENGINE_ERROR_CODE setVBucketState(uint16_t vbid, vbucket_state_t to,
                                      bool transfer) {
        return kvBucket->setVBucketState(vbid, to, transfer);
    }

    ~EventuallyPersistentEngine();

    engine_info *getInfo() {
        return &info.info;
    }

    item_info getItemInfo(const Item& itm);

    EPStats &getEpStats() {
        return stats;
    }

    KVBucket* getKVBucket() { return kvBucket.get(); }

    TapConnMap &getTapConnMap() { return *tapConnMap; }

    DcpConnMap &getDcpConnMap() { return *dcpConnMap_; }

    DcpFlowControlManager &getDcpFlowControlManager() {
        return *dcpFlowControlManager_;
    }

    TapConfig &getTapConfig() { return *tapConfig; }

    ReplicationThrottle &getReplicationThrottle() { return *replicationThrottle; }

    CheckpointConfig &getCheckpointConfig() { return *checkpointConfig; }

    SERVER_HANDLE_V1* getServerApi() { return serverApi; }

    Configuration &getConfiguration() {
        return configuration;
    }

    ENGINE_ERROR_CODE deregisterTapClient(const void* cookie,
                                          protocol_binary_request_header *request,
                                          ADD_RESPONSE response);

    ENGINE_ERROR_CODE handleCheckpointCmds(const void* cookie,
                                           protocol_binary_request_header *request,
                                           ADD_RESPONSE response);

    ENGINE_ERROR_CODE handleSeqnoCmds(const void* cookie,
                                      protocol_binary_request_header *request,
                                      ADD_RESPONSE response);

    ENGINE_ERROR_CODE resetReplicationChain(const void* cookie,
                                            protocol_binary_request_header *request,
                                            ADD_RESPONSE response);

    ENGINE_ERROR_CODE changeTapVBFilter(const void* cookie,
                                        protocol_binary_request_header *request,
                                        ADD_RESPONSE response);

    ENGINE_ERROR_CODE handleTrafficControlCmd(const void* cookie,
                                              protocol_binary_request_header *request,
                                              ADD_RESPONSE response);

    size_t getGetlDefaultTimeout() const {
        return getlDefaultTimeout;
    }

    size_t getGetlMaxTimeout() const {
        return getlMaxTimeout;
    }

    size_t getMaxFailoverEntries() const {
        return maxFailoverEntries;
    }

    bool isDegradedMode() const {
        return kvBucket->isWarmingUp() || !trafficEnabled.load();
    }

    WorkLoadPolicy &getWorkLoadPolicy(void) {
        return *workload;
    }

    bucket_priority_t getWorkloadPriority(void) const {return workloadPriority; }
    void setWorkloadPriority(bucket_priority_t p) { workloadPriority = p; }

    struct clusterConfig {
        std::string config;
        std::mutex lock;
    } clusterConfig;

    ENGINE_ERROR_CODE getRandomKey(const void *cookie,
                                   ADD_RESPONSE response);

    ConnHandler* getConnHandler(const void *cookie);

    void addLookupAllKeys(const void *cookie, ENGINE_ERROR_CODE err);

    /*
     * Explicitly trigger the defragmenter task. Provided to facilitate
     * testing.
     */
    void runDefragmenterTask(void);

    /*
     * Explicitly trigger the AccessScanner task. Provided to facilitate
     * testing.
     */
    bool runAccessScannerTask(void);

    /*
     * Explicitly trigger the VbStatePersist task. Provided to facilitate
     * testing.
     */
    void runVbStatePersistTask(int vbid);

    /**
     * Get a (sloppy) list of the sequence numbers for all of the vbuckets
     * on this server. It is not to be treated as a consistent set of seqence,
     * but rather a list of "at least" numbers. The way the list is generated
     * is that we're starting for vbucket 0 and record the current number,
     * then look at the next vbucket and record its number. That means that
     * at the time we get the number for vbucket X all of the previous
     * numbers could have been incremented. If the client just needs a list
     * of where we are for each vbucket this method may be more optimal than
     * requesting one by one.
     *
     * @param cookie The cookie representing the connection to requesting
     *               list
     * @param add_response The method used to format the output buffer
     * @return ENGINE_SUCCESS upon success
     */
    ENGINE_ERROR_CODE getAllVBucketSequenceNumbers(
                                        const void *cookie,
                                        protocol_binary_request_header *request,
                                        ADD_RESPONSE response);

    void updateDcpMinCompressionRatio(float value);

    /**
     * Sends a not-my-vbucket response, using the specified response callback.
     * to the specified connection via it's cookie.
     */
    ENGINE_ERROR_CODE sendNotMyVBucketResponse(ADD_RESPONSE response,
                                               const void* cookie,
                                               uint64_t cas);

    EpEngineTaskable& getTaskable() {
        return taskable;
    }

    unsigned int getArena() {
        return arenaId;
    }

    void setArena(arenaid_t arenaId) {
        this->arenaId = arenaId;
    }

protected:
    friend class EpEngineValueChangeListener;

    void setMaxItemSize(size_t value) {
        maxItemSize = value;
    }

    void setMaxItemPrivilegedBytes(size_t value) {
        maxItemPrivilegedBytes = value;
    }

    void setGetlDefaultTimeout(size_t value) {
        getlDefaultTimeout = value;
    }

    void setGetlMaxTimeout(size_t value) {
        getlMaxTimeout = value;
    }

    EventuallyPersistentEngine(GET_SERVER_API get_server_api);
    friend ENGINE_ERROR_CODE create_instance(uint64_t interface,
                                             GET_SERVER_API get_server_api,
                                             ENGINE_HANDLE **handle);
    uint16_t doWalkTapQueue(const void *cookie, item **itm, void **es,
                            uint16_t *nes, uint8_t *ttl, uint16_t *flags,
                            uint32_t *seqno, uint16_t *vbucket,
                            TapProducer *c, bool &retry);


    ENGINE_ERROR_CODE processTapAck(const void *cookie,
                                    uint32_t seqno,
                                    uint16_t status,
                                    const DocKey& key);

    /**
     * Report the state of a memory condition when out of memory.
     *
     * @return ETMPFAIL if we think we can recover without interaction,
     *         else ENOMEM
     */
    ENGINE_ERROR_CODE memoryCondition();

    /**
     * Check if there is any available memory space to allocate an Item
     * instance with a given size.
     */
    bool hasAvailableSpace(uint32_t nBytes) {
        return (stats.getTotalMemoryUsed() + nBytes) <= stats.getMaxDataSize();
    }

    friend class BGFetchCallback;
    friend class KVBucket;
    friend class EPBucket;

    bool enableTraffic(bool enable) {
        bool inverse = !enable;
        return trafficEnabled.compare_exchange_strong(inverse, enable);
    }

    ENGINE_ERROR_CODE doEngineStats(const void *cookie, ADD_STAT add_stat);
    ENGINE_ERROR_CODE doKlogStats(const void *cookie, ADD_STAT add_stat);
    ENGINE_ERROR_CODE doMemoryStats(const void *cookie, ADD_STAT add_stat);
    ENGINE_ERROR_CODE doVBucketStats(const void *cookie, ADD_STAT add_stat,
                                     const char* stat_key,
                                     int nkey,
                                     bool prevStateRequested,
                                     bool details);
    ENGINE_ERROR_CODE doHashStats(const void *cookie, ADD_STAT add_stat);
    ENGINE_ERROR_CODE doCheckpointStats(const void *cookie, ADD_STAT add_stat,
                                        const char* stat_key, int nkey);
    ENGINE_ERROR_CODE doTapStats(const void *cookie, ADD_STAT add_stat);
    ENGINE_ERROR_CODE doDcpStats(const void *cookie, ADD_STAT add_stat);
    ENGINE_ERROR_CODE doConnAggStats(const void *cookie, ADD_STAT add_stat,
                                     const char *sep, size_t nsep,
                                     conn_type_t connType);
    ENGINE_ERROR_CODE doTimingStats(const void *cookie, ADD_STAT add_stat);
    ENGINE_ERROR_CODE doSchedulerStats(const void *cookie, ADD_STAT add_stat);
    ENGINE_ERROR_CODE doRunTimeStats(const void *cookie, ADD_STAT add_stat);
    ENGINE_ERROR_CODE doDispatcherStats(const void *cookie, ADD_STAT add_stat);
    ENGINE_ERROR_CODE doTasksStats(const void* cookie, ADD_STAT add_stat);
    ENGINE_ERROR_CODE doKeyStats(const void *cookie, ADD_STAT add_stat,
                                 uint16_t vbid, const DocKey& key, bool validate=false);
    ENGINE_ERROR_CODE doTapVbTakeoverStats(const void *cookie,
                                           ADD_STAT add_stat,
                                           std::string& key,
                                           uint16_t vbid);

    ENGINE_ERROR_CODE doDcpVbTakeoverStats(const void *cookie,
                                           ADD_STAT add_stat,
                                           std::string &key,
                                           uint16_t vbid);
    ENGINE_ERROR_CODE doVbIdFailoverLogStats(const void *cookie,
                                             ADD_STAT add_stat,
                                             uint16_t vbid);
    ENGINE_ERROR_CODE doAllFailoverLogStats(const void *cookie, ADD_STAT add_stat);
    ENGINE_ERROR_CODE doWorkloadStats(const void *cookie, ADD_STAT add_stat);
    ENGINE_ERROR_CODE doSeqnoStats(const void *cookie, ADD_STAT add_stat,
                                   const char* stat_key, int nkey);
    void addSeqnoVbStats(const void *cookie, ADD_STAT add_stat,
                                  const RCPtr<VBucket> &vb);

    void addLookupResult(const void *cookie, Item *result) {
        LockHolder lh(lookupMutex);
        std::map<const void*, Item*>::iterator it = lookups.find(cookie);
        if (it != lookups.end()) {
            if (it->second != NULL) {
                LOG(EXTENSION_LOG_DEBUG,
                    "Cleaning up old lookup result for '%s'",
                    it->second->getKey().data());
                delete it->second;
            } else {
                LOG(EXTENSION_LOG_DEBUG, "Cleaning up old null lookup result");
            }
            lookups.erase(it);
        }
        lookups[cookie] = result;
    }

    bool fetchLookupResult(const void *cookie, Item **itm) {
        // This will return *and erase* the lookup result for a connection.
        // You look it up, you own it.
        LockHolder lh(lookupMutex);
        std::map<const void*, Item*>::iterator it = lookups.find(cookie);
        if (it != lookups.end()) {
            *itm = it->second;
            lookups.erase(it);
            return true;
        } else {
            return false;
        }
    }

    // Get the current tap connection for this cookie.
    // If this method returns NULL, you should return TAP_DISCONNECT
    TapProducer* getTapProducer(const void *cookie);

    // Initialize all required callbacks of this engine with the underlying
    // server.
    void initializeEngineCallbacks();

    /*
     * Private helper method for decoding the options on set/del_with_meta.
     * Tighly coupled to the logic of both those functions, it will
     * take a request pointer and locate and validate any options within.
     * @param request pointer to the set/del_with_meta request packet
     * @param generateCas set to Yes if CAS regeneration is enabled.
     * @param skipConflictResolution set to true if conflict resolution should
     *        not be performed.
     * @param keyOffset set to the number of bytes which are to be skipped to
     *        locate the key.
     */
    protocol_binary_response_status decodeWithMetaOptions(
                              protocol_binary_request_delete_with_meta* request,
                              GenerateCas& generateCas,
                              bool& skipConflictResolution,
                              int& keyOffset);

    /**
     * Sends NOT_SUPPORTED response, using the specified response callback
     * to the specified connection via it's cookie.
     *
     * @param response callback func to send the response
     * @param cookie conn cookie
     *
     * @return status of sending response
     */
    ENGINE_ERROR_CODE sendNotSupportedResponse(ADD_RESPONSE response,
                                               const void* cookie);

    /**
     * Sends error response, using the specified error and response callback
     * to the specified connection via it's cookie.
     *
     * @param response callback func to send the response
     * @param status error status to send
     * @param cas a cas value to send
     * @param cookie conn cookie
     *
     * @return status of sending response
     */
    ENGINE_ERROR_CODE sendErrorResponse(ADD_RESPONSE response,
                                        protocol_binary_response_status status,
                                        uint64_t cas,
                                        const void* cookie);

    /**
     * Sends a response that includes the mutation extras, the VB uuid and
     * seqno of the mutation.
     *
     * @param response callback func to send the response
     * @param vbucket vbucket that was mutated
     * @param bySeqno the seqno to send
     * @param status a mcbp status code
     * @param cas cas assigned to the mutation
     * @param cookie conn cookie
     * @returns NMVB if VB can't be located, or the ADD_RESPONSE return code.
     */
    ENGINE_ERROR_CODE sendMutationExtras(ADD_RESPONSE response,
                                         uint16_t vbucket,
                                         uint64_t bySeqno,
                                         protocol_binary_response_status status,
                                         uint64_t cas,
                                         const void* cookie);

    /**
     * Factory method for constructing the correct bucket type given the
     * configuration.
     * @param config Configuration to create bucket based on. Note this
     *               object may be modified to ensure the config is valid
     *               for the selected bucket type.
     */
    std::unique_ptr<KVBucket> makeBucket(Configuration& config);

    /**
     * helper method so that some commands can set the datatype of the document.
     *
     * @param cookie connection cookie
     * @param datatype the current document datatype
     * @param body a buffer containing the document body
     * @returns a datatype which will now include JSON if the document is JSON
     *          and the connection does not support datatype JSON.
     */
    protocol_binary_datatype_t checkForDatatypeJson(
            const void* cookie,
            protocol_binary_datatype_t datatype,
            cb::const_char_buffer body);

    /**
     * Process the set_with_meta with the given buffers/values.
     *
     * @param vbucket VB to mutate
     * @param key DocKey initialised with key data
     * @param value buffer for the mutation's value
     * @param itemMeta mutation's cas/revseq/flags/expiration
     * @param isDeleted the Item is deleted (with value)
     * @param datatype datatype of the mutation
     * @param cas [in,out] CAS for the command (updated with new CAS)
     * @param seqno [out] optional - returns the seqno allocated to the mutation
     * @param cookie connection's cookie
     * @param force Should the set skip conflict resolution?
     * @param allowExisting true if the set can overwrite existing key
     * @param genBySeqno generate a new seqno? (yes/no)
     * @param genCas generate a new CAS? (yes/no)
     * @param emd buffer referencing ExtendedMetaData
     * @returns state of the operation as an ENGINE_ERROR_CODE
     */
    ENGINE_ERROR_CODE setWithMeta(uint16_t vbucket,
                                  DocKey key,
                                  cb::const_byte_buffer value,
                                  ItemMetaData itemMeta,
                                  bool isDeleted,
                                  protocol_binary_datatype_t datatype,
                                  uint64_t& cas,
                                  uint64_t* seqno,
                                  const void* cookie,
                                  bool force,
                                  bool allowExisting,
                                  GenerateBySeqno genBySeqno,
                                  GenerateCas genCas,
                                  cb::const_byte_buffer emd);

    /**
     * Process the del_with_meta with the given buffers/values.
     *
     * @param vbucket VB to mutate
     * @param key DocKey initialised with key data
     * @param itemMeta mutation's cas/revseq/flags/expiration
     * @param cas [in,out] CAS for the command (updated with new CAS)
     * @param seqno [out] optional - returns the seqno allocated to the mutation
     * @param cookie connection's cookie
     * @param force Should the set skip conflict resolution?
     * @param genBySeqno generate a new seqno? (yes/no)
     * @param genCas generate a new CAS? (yes/no)
     * @param emd buffer referencing ExtendedMetaData
     * @returns state of the operation as an ENGINE_ERROR_CODE
     */
    ENGINE_ERROR_CODE deleteWithMeta(uint16_t vbucket,
                                     DocKey key,
                                     ItemMetaData itemMeta,
                                     uint64_t& cas,
                                     uint64_t* seqno,
                                     const void* cookie,
                                     bool force,
                                     GenerateBySeqno genBySeqno,
                                     GenerateCas genCas,
                                     cb::const_byte_buffer emd);

    SERVER_HANDLE_V1 *serverApi;
    std::unique_ptr<KVBucket> kvBucket;
    WorkLoadPolicy *workload;
    bucket_priority_t workloadPriority;

    ReplicationThrottle *replicationThrottle;
    std::map<const void*, Item*> lookups;
    std::unordered_map<const void*, ENGINE_ERROR_CODE> allKeysLookups;
    std::mutex lookupMutex;
    GET_SERVER_API getServerApiFunc;
    union {
        engine_info info;
        char buffer[sizeof(engine_info) + 10 * sizeof(feature_info) ];
    } info;

    DcpConnMap *dcpConnMap_;
    DcpFlowControlManager *dcpFlowControlManager_;
    TapConnMap *tapConnMap;
    TapConfig *tapConfig;
    CheckpointConfig *checkpointConfig;
    std::string name;
    size_t maxItemSize;
    size_t maxItemPrivilegedBytes;
    size_t getlDefaultTimeout;
    size_t getlMaxTimeout;
    size_t maxFailoverEntries;
    EPStats stats;
    Configuration configuration;
    std::atomic<bool> trafficEnabled;

    bool deleteAllEnabled;
    // a unique system generated token initialized at each time
    // ep_engine starts up.
    std::atomic<time_t> startupTime;
    EpEngineTaskable taskable;
    arenaid_t arenaId;
};

#endif  // SRC_EP_ENGINE_H_
