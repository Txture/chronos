package org.chronos.chronograph.internal.impl.structure.record;

import static com.google.common.base.Preconditions.*;

import java.util.*;
import java.util.Map.Entry;

import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.dump.annotations.ChronosExternalizable;
import org.chronos.chronograph.api.structure.ChronoEdge;
import org.chronos.chronograph.api.structure.record.IEdgeTargetRecord;
import org.chronos.chronograph.api.structure.record.IVertexPropertyRecord;
import org.chronos.chronograph.api.structure.record.IVertexRecord;
import org.chronos.chronograph.api.transaction.ChronoGraphTransaction;
import org.chronos.chronograph.internal.impl.dumpformat.converter.VertexRecordConverter;
import org.chronos.chronograph.internal.impl.structure.graph.ChronoEdgeImpl;
import org.chronos.chronograph.internal.impl.structure.graph.ChronoVertexImpl;
import org.chronos.chronograph.internal.impl.structure.graph.ChronoVertexProperty;
import org.chronos.chronograph.internal.impl.structure.record2.VertexRecord2;
import org.chronos.chronograph.internal.impl.util.ChronoProxyUtil;
import org.chronos.common.annotation.PersistentClass;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;

/**
 * A {@link VertexRecord} is the immutable data core of a vertex that has been persisted to the database.
 *
 * <p>
 * This is the class that will actually get serialized as the <code>value</code> in {@link ChronoDBTransaction#put(String, Object)}.
 *
 * <p>
 * It is crucial that all instances of this class are to be treated as immutable after their creation, as these instances are potentially shared among threads due to caching mechanisms.
 *
 * <p>
 * The {@link ChronoVertexImpl} implementation which typically wraps a {@link VertexRecord} is mutable and contains the transient (i.e. not yet persisted) state of the vertex that is specific for the transaction at hand. Upon calling {@link ChronoGraphTransaction#commit()}, the transient state in {@link ChronoVertexImpl} will be written into a new {@link VertexRecord} and persisted to the database with a new timestamp (but the same vertex id), provided that the vertex has indeed been modified by the user.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 * @deprecated Replaced by {@link VertexRecord2}. We keep this class to be able to read older graphs.
 */
@Deprecated
@PersistentClass("kryo")
@ChronosExternalizable(converterClass = VertexRecordConverter.class)
public final class VertexRecord implements IVertexRecord {

	// =====================================================================================================================
	// FIELDS
	// =====================================================================================================================

	// note: the only reason why the fields in this class are not declared as "final" is because
	// serialization mechanisms struggle with final fields. All fields are effectively final, and
	// all of their contents are effectively immutable.

	@SuppressWarnings("unused")
	/** The id of this record. Unused because new instances are serialized as {@link VertexRecord2}. */
	private String recordId;

	@SuppressWarnings("unused")
	/** The label of the vertex stored in this record. Unused because new instances are serialized as {@link VertexRecord2}. */
	private String label;

	@SuppressWarnings({"unused", "MismatchedQueryAndUpdateOfCollection"})
	/** Mapping of edge labels to incoming edges, i.e. edges which specify this vertex as their in-vertex. Unused because new instances are serialized as {@link VertexRecord2}. */
	private Map<String, Set<EdgeTargetRecord>> incomingEdges;

	@SuppressWarnings({"unused", "MismatchedQueryAndUpdateOfCollection"})
	/** Mapping of edge labels to outgoing edges, i.e. edges which specify this vertex as their out-vertex. Unused because new instances are serialized as {@link VertexRecord2}. */
	private Map<String, Set<EdgeTargetRecord>> outgoingEdges;

	@SuppressWarnings({"unused", "MismatchedQueryAndUpdateOfCollection"})
	/** The set of vertex properties known on this vertex. Unused because new instances are serialized as {@link VertexRecord2}. */
	private Set<VertexPropertyRecord> properties;

	// =====================================================================================================================
	// CONSTRUCTORS
	// =====================================================================================================================

	protected VertexRecord() {
		// default constructor for serialization mechanism
	}

	@Override
	public String getId() {
		return this.recordId;
	}

	@Override
	public String getLabel() {
		return this.label;
	}

	@Override
	public List<EdgeTargetRecordWithLabel> getIncomingEdges() {
		if (this.incomingEdges == null || this.incomingEdges.isEmpty()) {
			return Collections.emptyList();
		}
		List<EdgeTargetRecordWithLabel> resultList = new ArrayList<>();
		for(Entry<String, Set<EdgeTargetRecord>> entry : this.incomingEdges.entrySet()){
			String label = entry.getKey();
			Set<EdgeTargetRecord> edgeTargetRecords = entry.getValue();
			for(EdgeTargetRecord record : edgeTargetRecords){
				resultList.add(new EdgeTargetRecordWithLabel(record, label));
			}
		}
		return resultList;
	}

	@Override
	public List<EdgeTargetRecordWithLabel> getIncomingEdges(String... labels){
		if(labels == null || labels.length <= 0){
			return getIncomingEdges();
		}
		if(this.incomingEdges == null || this.incomingEdges.isEmpty()){
			return Collections.emptyList();
		}
		List<EdgeTargetRecordWithLabel> resultList = new ArrayList<>();
		for(String label : labels){
			Set<EdgeTargetRecord> labelRecords = this.incomingEdges.get(label);
			if(labelRecords == null){
				continue;
			}
			for(EdgeTargetRecord record : labelRecords){
				resultList.add(new EdgeTargetRecordWithLabel(record, label));
			}
		}
		return resultList;
	}

	@Override
	public SetMultimap<String, IEdgeTargetRecord> getIncomingEdgesByLabel() {
		if (this.incomingEdges == null || this.incomingEdges.isEmpty()) {
			// return the empty multimap
			return Multimaps.unmodifiableSetMultimap(HashMultimap.create());
		}
		// transform the internal data structure into a set multimap
		SetMultimap<String, IEdgeTargetRecord> multimap = HashMultimap.create();
		for (Entry<String, Set<EdgeTargetRecord>> entry : this.incomingEdges.entrySet()) {
			String label = entry.getKey();
			Set<EdgeTargetRecord> edges = entry.getValue();
			for (EdgeTargetRecord edge : edges) {
				multimap.put(label, edge);
			}
		}
		return Multimaps.unmodifiableSetMultimap(multimap);
	}

	@Override
	public List<EdgeTargetRecordWithLabel> getOutgoingEdges() {
		if (this.outgoingEdges == null || this.outgoingEdges.isEmpty()) {
			return Collections.emptyList();
		}
		List<EdgeTargetRecordWithLabel> resultList = new ArrayList<>();
		for(Entry<String, Set<EdgeTargetRecord>> entry : this.outgoingEdges.entrySet()){
			String label = entry.getKey();
			Set<EdgeTargetRecord> edgeTargetRecords = entry.getValue();
			for(EdgeTargetRecord record : edgeTargetRecords){
				resultList.add(new EdgeTargetRecordWithLabel(record, label));
			}
		}
		return resultList;
	}

	@Override
	public List<EdgeTargetRecordWithLabel> getOutgoingEdges(String... labels){
		if(labels == null || labels.length <= 0){
			return getOutgoingEdges();
		}
		if(this.outgoingEdges == null || this.outgoingEdges.isEmpty()){
			return Collections.emptyList();
		}
		List<EdgeTargetRecordWithLabel> resultList = new ArrayList<>();
		for(String label : labels){
			Set<EdgeTargetRecord> labelRecords = this.outgoingEdges.get(label);
			if(labelRecords == null){
				continue;
			}
			for(EdgeTargetRecord record : labelRecords){
				resultList.add(new EdgeTargetRecordWithLabel(record, label));
			}
		}
		return resultList;
	}

	@Override
	public SetMultimap<String, IEdgeTargetRecord> getOutgoingEdgesByLabel() {
		if (this.outgoingEdges == null || this.outgoingEdges.isEmpty()) {
			// return the empty multimap
			return Multimaps.unmodifiableSetMultimap(HashMultimap.create());
		}
		// transform the internal data structure into a set multimap
		SetMultimap<String, IEdgeTargetRecord> multimap = HashMultimap.create();
		for (Entry<String, Set<EdgeTargetRecord>> entry : this.outgoingEdges.entrySet()) {
			String label = entry.getKey();
			Set<EdgeTargetRecord> edges = entry.getValue();
			for (EdgeTargetRecord edge : edges) {
				multimap.put(label, edge);
			}
		}
		return Multimaps.unmodifiableSetMultimap(multimap);
	}

	@Override
	public Set<IVertexPropertyRecord> getProperties() {
		if (this.properties == null || this.properties.isEmpty()) {
			return Collections.emptySet();
		}
		return Collections.unmodifiableSet(this.properties);
	}

	@Override
	public VertexPropertyRecord getProperty(final String propertyKey) {
		if(propertyKey == null || this.properties == null || this.properties.isEmpty()){
			return null;
		}
		for(VertexPropertyRecord record : this.properties){
			if(Objects.equals(record.getKey(), propertyKey)){
				return record;
			}
		}
		return null;
	}

}
