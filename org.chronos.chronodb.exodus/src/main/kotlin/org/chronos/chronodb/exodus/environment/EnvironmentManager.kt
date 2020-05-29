package org.chronos.chronodb.exodus.environment

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.backup.BackupStrategy
import jetbrains.exodus.crypto.StreamCipherProvider
import jetbrains.exodus.env.*
import jetbrains.exodus.management.Statistics
import org.chronos.chronodb.exodus.kotlin.ext.mapSingle
import org.chronos.common.logging.ChronoLogger
import java.io.Closeable
import java.io.File
import java.lang.IllegalStateException
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReadWriteLock
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.withLock

class EnvironmentManager : Closeable{

    @Volatile
    private var isClosed = false

    private val xodusConfiguration: Map<String, Any>

    private val environmentProxies: MutableMap<File, EnvironmentProxy>
    private val keepOpenEnvironments: Int

    private val cleanupExecutor: ScheduledExecutorService

    constructor(xodusConfiguration: Map<String, Any>, keepOpenEnvironments: Int, cleanupPeriodSeconds: Int) {
        this.xodusConfiguration = xodusConfiguration
        this.environmentProxies = ConcurrentHashMap()
        this.keepOpenEnvironments = keepOpenEnvironments
        this.cleanupExecutor = Executors.newSingleThreadScheduledExecutor()
        this.cleanupExecutor.scheduleWithFixedDelay(this::performCleanup, 0L, cleanupPeriodSeconds.toLong(), TimeUnit.SECONDS)
    }

    override fun close() {
        if(this.isClosed){
            return
        }
        this.isClosed = true
        this.cleanupExecutor.shutdownNow()
        this.cleanupEnvironments(true)
        this.environmentProxies.clear()
    }

    fun getEnvironment(file: File): Environment {
        assertNotClosed()
        return this.environmentProxies.computeIfAbsent(file) { EnvironmentProxy(file) }
    }

    private fun getEnvironmentForProxy(proxy: EnvironmentProxy) : Environment {
        assertNotClosed()
        ChronoLogger.logDebug("Opening Environment ${proxy.file.absolutePath}")
        val config = EnvironmentConfig{ key ->
            // note: The Exodus API demands the value to be a string here.
            // Internally, they parse that string again...
            this.xodusConfiguration[key]?.toString()
        }
        return Environments.newInstance(proxy.file, config)
    }

    private fun performCleanup(){
        var closedEnv = 0
        var openEnv = 0
        this.environmentProxies.values.asSequence()
                .sortedByDescending { it.lastAccessedAtSystemTime }
                .drop(this.keepOpenEnvironments)
                .forEach {
                    val closed = it.releaseEnvironmentIfPossible()
                    if(closed){
                        closedEnv++
                    }else{
                        openEnv++
                    }
                }
    }

    fun cleanupEnvironments(force: Boolean){
        environmentProxies.values.forEach { envProxy ->
            if(force){
                envProxy.releaseEnvironmentForce()
            }else{
                envProxy.releaseEnvironmentIfPossible()
            }
        }
    }

    private fun assertNotClosed(){
        if(this.isClosed){
            throw IllegalStateException("Environment manager has already been closed!")
        }
    }

    private inner class EnvironmentProxy : Environment {

        val file: File
        var environment: Environment?

        var openTxCount: AtomicInteger = AtomicInteger(0)

        val lock: ReadWriteLock

        @Volatile
        var lastAccessedAtSystemTime = System.currentTimeMillis()

        constructor(file: File) {
            this.file = file
            this.environment = null
            this.lock = ReentrantReadWriteLock(false)
        }

        @Synchronized
        private fun env(): Environment {
            val isOpen = this.environment?.isOpen ?: false
            if (!isOpen) {
                // get the real environment from the manager
                this.environment = getEnvironmentForProxy(this)
            }
            return this.environment!!
        }

        override fun executeTransactionSafeTask(task: Runnable) {
            this.inReadLock { env ->
                env.executeTransactionSafeTask(task)
            }
        }

        override fun getCipherProvider(): StreamCipherProvider? {
            return this.inReadLock { env ->
                env.cipherProvider
            }
        }

        override fun clear() {
            this.inReadLock { env ->
                env.clear()
            }
        }

        override fun removeStore(storeName: String, txn: Transaction) {
            this.inReadLock { env ->
                env.removeStore(storeName, txn.unproxy())
            }
        }

        override fun resumeGC() {
            this.inReadLock { env ->
                env.resumeGC()
            }
        }

        override fun getBackupStrategy(): BackupStrategy {
            return this.inReadLock { env ->
                env.backupStrategy
            }
        }

        override fun executeInTransaction(executable: TransactionalExecutable) {
            this.inReadLock { env ->
                env.executeInTransaction(executable)
            }
        }

        override fun getStatistics(): Statistics<out Enum<*>> {
            return this.inReadLock { env ->
                env.statistics
            }
        }

        override fun getCipherBasicIV(): Long {
            return this.inReadLock { env ->
                env.cipherBasicIV
            }
        }

        override fun beginTransaction(): Transaction {
            return this.inReadLock { env ->
                this.openTxCount.incrementAndGet()
                TransactionProxy(env.beginTransaction())
            }
        }

        override fun beginTransaction(beginHook: Runnable?): Transaction {
            return this.inReadLock { env ->
                this.openTxCount.incrementAndGet()
                TransactionProxy(env.beginTransaction(beginHook))
            }
        }

        override fun suspendGC() {
            this.inReadLock { env ->
                env.suspendGC()
            }
        }

        override fun beginReadonlyTransaction(): Transaction {
            return this.inReadLock { env ->
                this.openTxCount.incrementAndGet()
                TransactionProxy(env.beginReadonlyTransaction())
            }
        }

        override fun beginReadonlyTransaction(beginHook: Runnable?): Transaction {
            return this.inReadLock { env ->
                this.openTxCount.incrementAndGet()
                TransactionProxy(env.beginReadonlyTransaction(beginHook))
            }
        }

        override fun getCreated(): Long {
            return this.inReadLock { env ->
                this.env().created
            }
        }

        override fun isOpen(): Boolean {
            // environment proxies are always open
            return true
        }

        override fun close() {
            throw UnsupportedOperationException("Close() is not supported on Environment proxies!")
        }

        override fun getEnvironmentConfig(): EnvironmentConfig {
            return this.inReadLock { env ->
                return env.environmentConfig
            }
        }

        override fun getLocation(): String {
            return this.inReadLock { env ->
                env.location
            }
        }

        override fun storeExists(storeName: String, txn: Transaction): Boolean {
            return this.inReadLock { env ->
                env.storeExists(storeName, txn.unproxy())
            }
        }

        override fun gc() {
            this.inReadLock { env ->
                env.gc()
            }
        }

        override fun toString(): String {
            return "EnvProxy[${this.file.absolutePath}]"
        }

        override fun executeInReadonlyTransaction(executable: TransactionalExecutable) {
            this.inReadLock { env ->
                env.executeInReadonlyTransaction(executable)
            }
        }

        override fun <T : Any?> computeInExclusiveTransaction(computable: TransactionalComputable<T>): T {
            return this.inReadLock { env ->
                env.computeInExclusiveTransaction(computable)
            }
        }

        override fun getCipherKey(): ByteArray? {
            return this.inReadLock { env ->
                env.cipherKey
            }
        }

        override fun openStore(name: String, config: StoreConfig, transaction: Transaction): Store {
            return this.inReadLock { env ->
                StoreProxy(env.openStore(name, config, transaction.unproxy()))
            }
        }

        override fun openStore(name: String, config: StoreConfig, transaction: Transaction, creationRequired: Boolean): Store? {
            return this.inReadLock { env ->
                env.openStore(name, config, transaction.unproxy(), creationRequired).mapSingle { StoreProxy(it) }
            }
        }

        override fun <T : Any?> computeInReadonlyTransaction(computable: TransactionalComputable<T>): T {
            return this.inReadLock { env ->
                env.computeInReadonlyTransaction(computable)
            }
        }

        override fun <T : Any?> computeInTransaction(computable: TransactionalComputable<T>): T {
            return this.inReadLock { env ->
                env.computeInTransaction(computable)
            }
        }

        override fun executeInExclusiveTransaction(executable: TransactionalExecutable) {
            return this.inReadLock { env ->
                env.executeInExclusiveTransaction(executable)
            }
        }

        override fun beginExclusiveTransaction(): Transaction {
            return this.inReadLock { env ->
                this.openTxCount.incrementAndGet()
                TransactionProxy(env.beginExclusiveTransaction())
            }
        }

        override fun beginExclusiveTransaction(beginHook: Runnable?): Transaction {
            return this.inReadLock { env ->
                this.openTxCount.incrementAndGet()
                TransactionProxy(env.beginExclusiveTransaction(beginHook))
            }
        }

        override fun getAllStoreNames(txn: Transaction): MutableList<String> {
            return this.inReadLock { env ->
                env.getAllStoreNames(txn.unproxy())
            }
        }

        override fun truncateStore(storeName: String, txn: Transaction) {
            return this.inReadLock { env ->
                env.truncateStore(storeName, txn.unproxy())
            }
        }

        private fun onTxFinished() {
            this.inReadLock {
                this.openTxCount.decrementAndGet()
            }
        }

        fun releaseEnvironmentForce() {
            this.lock.writeLock().withLock {
                val myEnv = this.environment
                val isOpen = myEnv?.isOpen ?: false
                if(myEnv != null && isOpen){
                    // we want open transactions to be canceled
                    myEnv.environmentConfig.envCloseForcedly = true
                    myEnv.close()
                }
            }
        }

        fun releaseEnvironmentIfPossible(): Boolean {
            val acquiredLock = this.lock.writeLock().tryLock()
            if(!acquiredLock){
                // the environment is in active use -> don't close it.
                return false
            }
            try{
                if (this.openTxCount.get() > 0) {
                    return false
                }
                val myEnv = this.environment
                val isOpen = myEnv?.isOpen ?: false
                if (myEnv != null && isOpen) {
                    ChronoLogger.log("Environment ${myEnv.location} will be closed.")
                    myEnv.close()
                }
                return true
            }finally{
                this.lock.writeLock().unlock()
            }
        }

        inline fun <R> inReadLock(task: (Environment) -> R): R {
            this.lock.readLock().withLock {
                val taskResult =  task(this.env())
                this.lastAccessedAtSystemTime = System.currentTimeMillis()
                return taskResult
            }
        }

        fun Transaction.unproxy(): Transaction {
            return when(this){
                is TransactionProxy -> this.tx
                else -> this
            }
        }

        private inner class TransactionProxy(val tx: Transaction) : Transaction by tx {

            override fun abort() {
                this.tx.abort()
                this@EnvironmentProxy.onTxFinished()
            }

            override fun commit(): Boolean {
                val result = this.tx.commit()
                this@EnvironmentProxy.onTxFinished()
                return result
            }

        }

        private inner class StoreProxy(val store: Store): Store {

            override fun putRight(txn: Transaction, key: ByteIterable, value: ByteIterable) {
                this.store.putRight(txn.unproxy(), key, value)
            }

            override fun put(txn: Transaction, key: ByteIterable, value: ByteIterable): Boolean {
                return this.store.put(txn.unproxy(), key, value)
            }

            override fun getName(): String {
                return this.store.name
            }

            override fun openCursor(txn: Transaction): Cursor {
                return this.store.openCursor(txn.unproxy())
            }

            override fun add(txn: Transaction, key: ByteIterable, value: ByteIterable): Boolean {
                return this.store.add(txn.unproxy(), key, value)
            }

            override fun count(txn: Transaction): Long {
                return this.store.count(txn.unproxy())
            }

            override fun getEnvironment(): Environment {
               return this.store.environment
            }

            override fun getConfig(): StoreConfig {
                return this.store.config
            }

            override fun get(txn: Transaction, key: ByteIterable): ByteIterable? {
                return this.store.get(txn.unproxy(), key)
            }

            override fun exists(txn: Transaction, key: ByteIterable, value: ByteIterable): Boolean {
                return this.store.exists(txn.unproxy(), key, value)
            }

            override fun close() {
                return this.store.close()
            }

            override fun delete(txn: Transaction, key: ByteIterable): Boolean {
                return this.store.delete(txn.unproxy(), key)
            }

        }
    }


}

