package org.chronos.chronograph.test.manual

import com.google.common.base.Preconditions
import com.google.common.collect.Lists
import mu.KotlinLogging
import org.apache.tinkerpop.shaded.jackson.databind.JsonNode
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper
import org.apache.tinkerpop.shaded.jackson.databind.node.JsonNodeFactory
import org.apache.tinkerpop.shaded.jackson.databind.node.NullNode
import org.apache.tinkerpop.shaded.jackson.databind.node.ObjectNode
import org.chronos.chronodb.api.DumpOption
import org.chronos.chronodb.api.dump.ChronoDBDumpFormat
import org.chronos.chronodb.inmemory.InMemorySerializationManager
import org.chronos.chronodb.internal.api.stream.ChronoDBEntry
import org.chronos.chronodb.internal.api.stream.ObjectInput
import org.chronos.chronodb.internal.impl.dump.ChronoDBDumpUtil
import org.chronos.chronodb.internal.impl.dump.CommitMetadataMap
import org.chronos.chronodb.internal.impl.dump.ConverterRegistry
import org.chronos.chronodb.internal.impl.dump.DumpOptions
import org.chronos.chronodb.internal.impl.dump.base.ChronoDBDumpElement
import org.chronos.chronodb.internal.impl.dump.entry.ChronoDBDumpEntry
import org.chronos.chronodb.internal.impl.dump.meta.ChronoDBDumpMetadata
import org.chronos.chronograph.api.structure.record.IEdgeRecord
import org.chronos.chronograph.api.structure.record.IEdgeTargetRecord
import org.chronos.chronograph.api.structure.record.IVertexRecord
import org.chronos.chronograph.internal.impl.dumpformat.GraphDumpFormat
import org.chronos.common.serialization.KryoManager
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import java.io.File

class DumpToJsonTest {

    companion object {

        private val log = KotlinLogging.logger {}

    }

    @Test
    @Disabled("this is only used for the purposes of manually dumping a graph into JSON format.")
    fun convertDumpToJson() {
        val exportFile = File("/mnt/382eae86-8ab8-4d0d-b8e3-b343cc7b5547/TxtureHomes/txtureHomeTaxonomyExport/export.xml.gz")
        val outputFile = File("/mnt/382eae86-8ab8-4d0d-b8e3-b343cc7b5547/chronos-ng-testdata/export.jsonl")
        outputFile.bufferedWriter().use { fileWriter ->
            val options = DumpOptions(DumpOption.ENABLE_GZIP)
            GraphDumpFormat.registerGraphAliases(options)
            GraphDumpFormat.registerDefaultConvertersForReading(options)
            ChronoDBDumpFormat.createInput(exportFile, options).use { input ->
                // create the converter registry based on the options
                val converters = ConverterRegistry(options)
                // the first element should ALWAYS be the metadata
                val metadata = ChronoDBDumpUtil.readMetadata(input)
                val objectMapper = ObjectMapper()
                var batchIndex = 0
                var writtenEntries = 0
                loadEntries(input, metadata, converters, options) { entryBatch ->
                    println("Writing batch #${batchIndex} (entries written so far: ${writtenEntries})")
                    batchIndex++
                    entryBatch.forEach {
                        val deserialized = if (it.value.isEmpty()) {
                            // deletion
                            null
                        } else {
                            KryoManager.deserialize<Any>(it.value)
                        }
                        val json = when (deserialized) {
                            null -> NullNode.getInstance()
                            is IVertexRecord -> jsonizeVertexRecord(deserialized, objectMapper)
                            is IEdgeRecord -> jsonizeEdgeRecord(deserialized, objectMapper)
                            else -> return@forEach // skip this thing
                        }
                        val jsonEntry = createJsonEntry(it.identifier.keyspace, it.identifier.key, it.identifier.timestamp, json)
                        val jsonEntryString = objectMapper.writeValueAsString(jsonEntry).replace("""\r?\n""", " ").replace("""\s+""", " ")
                        fileWriter.write(jsonEntryString)
                        fileWriter.write("\n")
                        writtenEntries++
                    }
                }
            }
        }
    }

    private fun createJsonEntry(keyspace: String, key: String, timestamp: Long, json: JsonNode): JsonNode {
        val jnf = JsonNodeFactory.instance
        val rootNode = jnf.objectNode()
        rootNode.put("keyspace", keyspace)
        rootNode.put("key", key)
        rootNode.put("timestamp", timestamp)
        rootNode.set<JsonNode>("value", json)
        return rootNode
    }

    private fun jsonizeVertexRecord(record: IVertexRecord, objectMapper: ObjectMapper): ObjectNode {
        val jnf = JsonNodeFactory.instance
        val rootNode = jnf.objectNode()
        rootNode.put("id", record.id)
        rootNode.put("label", record.label)
        if(!record.properties.isNullOrEmpty()){
            val propertiesNode = jnf.objectNode()
            record.properties.forEach { property ->
                val propertyValueNode = jnf.objectNode()
                propertyValueNode.set<JsonNode>("value", objectMapper.valueToTree(property.serializationSafeValue))
                if(!property.properties.isNullOrEmpty()){
                    val metaPropertiesNode = jnf.objectNode()
                    property.properties.forEach { (key, metaPropRecord) ->
                        val valueNode = objectMapper.valueToTree<JsonNode>(metaPropRecord.serializationSafeValue)
                        metaPropertiesNode.set<JsonNode>(key, valueNode)
                    }
                    propertyValueNode.set<ObjectNode>("metaProperties", metaPropertiesNode)
                }
                propertiesNode.set<JsonNode>(property.key, propertyValueNode)
            }
            rootNode.set<ObjectNode>("properties", propertiesNode)
        }
        if(record.incomingEdgesByLabel != null && !record.incomingEdgesByLabel.isEmpty){
            val inENode = jnf.objectNode()
            for ((label, edgeTargetRecords) in record.incomingEdgesByLabel.asMap()) {
                val array = jnf.arrayNode()
                edgeTargetRecords.map { this.edgeTargetRecordToObjectNode(it) }.forEach { array.add(it) }
                inENode.set<JsonNode>(label, array)
            }
            rootNode.set<JsonNode>("inE", inENode)
        }
        if(record.outgoingEdgesByLabel != null && !record.outgoingEdgesByLabel.isEmpty){
            val outENode = jnf.objectNode()
            for((label, edgeTargetRecords) in record.outgoingEdgesByLabel.asMap()) {
                val array = jnf.arrayNode()
                edgeTargetRecords.map { this.edgeTargetRecordToObjectNode(it) }.forEach { array.add(it) }
                outENode.set<JsonNode>(label, array)
            }
            rootNode.set<JsonNode>("outE", outENode)
        }
        return rootNode
    }

    private fun edgeTargetRecordToObjectNode(record: IEdgeTargetRecord): ObjectNode {
        val jnf = JsonNodeFactory.instance
        val objectNode = jnf.objectNode()
        objectNode.put("edgeId", record.edgeId)
        objectNode.put("otherEndVertexId", record.otherEndVertexId)
        return objectNode
    }

    private fun jsonizeEdgeRecord(record: IEdgeRecord, objectMapper: ObjectMapper): ObjectNode {
        val jnf = JsonNodeFactory.instance
        val rootNode = jnf.objectNode()
        rootNode.put("id", record.id)
        rootNode.put("label", record.label)
        rootNode.put("inV", record.inVertexId)
        rootNode.put("outV", record.outVertexId)
        if(!record.properties.isNullOrEmpty()){
            val propertiesNode = jnf.objectNode()
            record.properties.forEach { property ->
                propertiesNode.set<JsonNode>(property.key, objectMapper.valueToTree(property.serializationSafeValue))
            }
            rootNode.set<ObjectNode>("properties", propertiesNode)
        }
        return rootNode
    }


    private fun loadEntries(
        input: ObjectInput,
        metadata: ChronoDBDumpMetadata,
        converters: ConverterRegistry,
        options: DumpOptions,
        saveEntries: (List<ChronoDBEntry>) -> Unit,
    ) {
        Preconditions.checkNotNull(input, "Precondition violation - argument 'input' must not be NULL!")
        Preconditions.checkNotNull(metadata, "Precondition violation - argument 'metadata' must not be NULL!")
        Preconditions.checkNotNull(converters, "Precondition violation - argument 'converters' must not be NULL!")
        Preconditions.checkNotNull(options, "Precondition violation - argument 'options' must not be NULL!")
        val sm = InMemorySerializationManager()
        // this is our read batch. We fill it one by one, when it's full, we load that batch into the DB.
        val readBatch: MutableList<ChronoDBEntry> = Lists.newArrayList()
        val batchSize = options.batchSize
        // we also maintain a list of encountered commit timestamps.
        val commitMetadataMap = CommitMetadataMap()
        // copy over the commits we obtained from the commit metadata map (if any)
        val commitDumpMetadata = metadata.commitDumpMetadata
        for (commit in commitDumpMetadata) {
            commitMetadataMap.addEntry(commit.branch, commit.timestamp, commit.metadata)
        }
        while (input.hasNext()) {
            val element = input.next() as ChronoDBDumpElement
            // this element should be an entry...
            if (element !is ChronoDBDumpEntry<*>) {
                // hm... no idea what this could be.
                log.error("Encountered unexpected element of type '" + element.javaClass.name
                    + "', expected '" + ChronoDBDumpEntry::class.java.name + "'. Skipping this entry.")
                continue
            }
            // cast down to the entry and check what it is
            val entry = ChronoDBDumpUtil.convertDumpEntryToDBEntry(element, sm, converters)
            readBatch.add(entry)
            commitMetadataMap.addEntry(entry.identifier)
            // check if we need to flush our read batch into the DB
            if (readBatch.size >= batchSize) {
                if (log.isDebugEnabled) {
                    log.debug("Reading a batch of size $batchSize")
                }
                saveEntries(readBatch)
                readBatch.clear()
            }
        }
        // we are at the end of the input; flush the remaining buffer (if any)
        if (readBatch.isNotEmpty()) {
            saveEntries(readBatch)
            readBatch.clear()
        }
    }

}