package org.chronos.chronograph.internal.impl.structure.record3;

import com.google.common.collect.*;
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
import org.chronos.chronograph.internal.impl.structure.record.EdgeTargetRecordWithLabel;
import org.chronos.chronograph.internal.impl.structure.record2.EdgeTargetRecord2;
import org.chronos.chronograph.internal.impl.util.ChronoProxyUtil;
import org.chronos.common.annotation.PersistentClass;

import java.util.*;
import java.util.Map.Entry;

import static com.google.common.base.Preconditions.*;

/**
 * A {@link VertexRecord3} is the immutable data core of a vertex that has been persisted to the database.
 *
 * <p>
 * This is the class that will actually get serialized as the <code>value</code> in {@link ChronoDBTransaction#put(String, Object)}.
 *
 * <p>
 * It is crucial that all instances of this class are to be treated as immutable after their creation, as these instances are potentially shared among threads due to caching mechanisms.
 *
 * <p>
 * The {@link ChronoVertexImpl} implementation which typically wraps a {@link VertexRecord3} is mutable and contains the transient (i.e. not yet persisted) state of the vertex that is specific for the transaction at hand. Upon calling {@link ChronoGraphTransaction#commit()}, the transient state in {@link ChronoVertexImpl} will be written into a new {@link VertexRecord3} and persisted to the database with a new timestamp (but the same vertex id), provided that the vertex has indeed been modified by the user.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
@PersistentClass("kryo")
@ChronosExternalizable(converterClass = VertexRecordConverter.class)
public final class VertexRecord3 implements IVertexRecord {

	// =====================================================================================================================
	// FIELDS
	// =====================================================================================================================

	// note: the only reason why the fields in this class are not declared as "final" is because
	// serialization mechanisms struggle with final fields. All fields are effectively final, and
	// all of their contents are effectively immutable.

	/** The id of this record. */
	private String recordId;
	/** The label of the vertex stored in this record. */
	private String label;
	/** Mapping of edge labels to incoming edges, i.e. edges which specify this vertex as their in-vertex. */
	private Map<String, Set<EdgeTargetRecord2>> incomingEdges;
	/** Mapping of edge labels to outgoing edges, i.e. edges which specify this vertex as their out-vertex. */
	private Map<String, Set<EdgeTargetRecord2>> outgoingEdges;
	/** The set of vertex properties known on this vertex. */
	private Set<IVertexPropertyRecord> properties;

	// =====================================================================================================================
	// CONSTRUCTORS
	// =====================================================================================================================

	protected VertexRecord3() {
		// default constructor for serialization mechanism
	}

	public VertexRecord3(final String recordId, final String label, final SetMultimap<String, ChronoEdge> inE,
                         final SetMultimap<String, ChronoEdge> outE, final Map<String, ChronoVertexProperty<?>> properties) {
		checkNotNull(recordId, "Precondition violation - argument 'recordId' must not be NULL!");
		checkNotNull(label, "Precondition violation - argument 'label' must not be NULL!");
		checkNotNull(properties, "Precondition violation - argument 'properties' must not be NULL!");
		this.recordId = recordId;
		this.label = label;
		if (inE != null && inE.isEmpty() == false) {
			this.incomingEdges = Maps.newHashMap();
			Iterator<ChronoEdge> inEIterator = Sets.newHashSet(inE.values()).iterator();
			while (inEIterator.hasNext()) {
				ChronoEdgeImpl edge = ChronoProxyUtil.resolveEdgeProxy(inEIterator.next());
				// create the minimal "edge target" representation for this edge to store in this vertex
				EdgeTargetRecord2 edgeTargetRecord = new EdgeTargetRecord2(edge.id(), edge.outVertex().id());
				// retrieve the set of edge target records with the label in question
				Set<EdgeTargetRecord2> edgeRecordsByLabel = this.incomingEdges.get(edge.label());
				if (edgeRecordsByLabel == null) {
					// this is the first edge with that label; add a new set to the map
					edgeRecordsByLabel = Sets.newHashSet();
					this.incomingEdges.put(edge.label(), edgeRecordsByLabel);
				}
				// store the edge target record by labels
				edgeRecordsByLabel.add(edgeTargetRecord);
			}
		}
		if (outE != null && outE.isEmpty() == false) {
			this.outgoingEdges = Maps.newHashMap();
			Iterator<ChronoEdge> outEIterator = Sets.newHashSet(outE.values()).iterator();
			while (outEIterator.hasNext()) {
				ChronoEdgeImpl edge = ChronoProxyUtil.resolveEdgeProxy(outEIterator.next());
				// create the minimal "edge target" representation for this edge to store in this vertex
				EdgeTargetRecord2 edgeTargetRecord = new EdgeTargetRecord2(edge.id(), edge.inVertex().id());
				// retrieve the set of edge target records with the label in question
				Set<EdgeTargetRecord2> edgeRecordsByLabel = this.outgoingEdges.get(edge.label());
				if (edgeRecordsByLabel == null) {
					// this is the first edge with that label; add a new set to the map
					edgeRecordsByLabel = Sets.newHashSet();
					this.outgoingEdges.put(edge.label(), edgeRecordsByLabel);
				}
				// store the edge target record by labels
				edgeRecordsByLabel.add(edgeTargetRecord);
			}
		}
		// create an immutable copy of the vertex properties
		Collection<ChronoVertexProperty<?>> props = properties.values();
		if (props.isEmpty() == false) {
			// we have at least one property
			this.properties = Sets.newHashSet();
			for (ChronoVertexProperty<?> property : props) {
				IVertexPropertyRecord pRecord = property.toRecord();
				this.properties.add(pRecord);
			}
		}
	}

	public VertexRecord3(final String recordId, final String label, final SetMultimap<String, EdgeTargetRecord2> inE,
                         final SetMultimap<String, EdgeTargetRecord2> outE, final Set<IVertexPropertyRecord> properties) {
		checkNotNull(recordId, "Precondition violation - argument 'recordId' must not be NULL!");
		checkNotNull(label, "Precondition violation - argument 'label' must not be NULL!");
		checkNotNull(inE, "Precondition violation - argument 'inE' must not be NULL!");
		checkNotNull(outE, "Precondition violation - argument 'outE' must not be NULL!");
		checkNotNull(properties, "Precondition violation - argument 'properties' must not be NULL!");
		this.recordId = recordId;
		this.label = label;
		// convert incoming edges
		if (inE.isEmpty() == false) {
			this.incomingEdges = Maps.newHashMap();
			for (Entry<String, Collection<EdgeTargetRecord2>> entry : inE.asMap().entrySet()) {
				String edgeLabel = entry.getKey();
				Collection<EdgeTargetRecord2> edgeRecords = entry.getValue();
				if (edgeRecords.isEmpty()) {
					continue;
				}
				Set<EdgeTargetRecord2> edgeRecordsByLabel = this.incomingEdges.get(edgeLabel);
				if (edgeRecordsByLabel == null) {
					// this is the first edge with that label; add a new set to the map
					edgeRecordsByLabel = Sets.newHashSet();
					this.incomingEdges.put(edgeLabel, edgeRecordsByLabel);
				}
				edgeRecordsByLabel.addAll(edgeRecords);
			}
		}
		// convert outgoing edges
		if (outE.isEmpty() == false) {
			this.outgoingEdges = Maps.newHashMap();
			for (Entry<String, Collection<EdgeTargetRecord2>> entry : outE.asMap().entrySet()) {
				String edgeLabel = entry.getKey();
				Collection<EdgeTargetRecord2> edgeRecords = entry.getValue();
				if (edgeRecords.isEmpty()) {
					continue;
				}
				Set<EdgeTargetRecord2> edgeRecordsByLabel = this.outgoingEdges.get(edgeLabel);
				if (edgeRecordsByLabel == null) {
					// this is the first edge with that label; add a new set to the map
					edgeRecordsByLabel = Sets.newHashSet();
					this.outgoingEdges.put(edgeLabel, edgeRecordsByLabel);
				}
				edgeRecordsByLabel.addAll(edgeRecords);
			}
		}
		// convert vertex properties
		if (properties.isEmpty() == false) {
			this.properties = Sets.newHashSet();
			this.properties.addAll(properties);
		}
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
		for(Entry<String, Set<EdgeTargetRecord2>> entry : this.incomingEdges.entrySet()){
			String label = entry.getKey();
			Set<EdgeTargetRecord2> edgeTargetRecords = entry.getValue();
			for(EdgeTargetRecord2 record : edgeTargetRecords){
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
			Set<EdgeTargetRecord2> labelRecords = this.incomingEdges.get(label);
			if(labelRecords == null){
				continue;
			}
			for(EdgeTargetRecord2 record : labelRecords){
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
		for (Entry<String, Set<EdgeTargetRecord2>> entry : this.incomingEdges.entrySet()) {
			String label = entry.getKey();
			Set<EdgeTargetRecord2> edges = entry.getValue();
			for (EdgeTargetRecord2 edge : edges) {
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
		for(Entry<String, Set<EdgeTargetRecord2>> entry : this.outgoingEdges.entrySet()){
			String label = entry.getKey();
			Set<EdgeTargetRecord2> edgeTargetRecords = entry.getValue();
			for(EdgeTargetRecord2 record : edgeTargetRecords){
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
			Set<EdgeTargetRecord2> labelRecords = this.outgoingEdges.get(label);
			if(labelRecords == null){
				continue;
			}
			for(EdgeTargetRecord2 record : labelRecords){
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
		for (Entry<String, Set<EdgeTargetRecord2>> entry : this.outgoingEdges.entrySet()) {
			String label = entry.getKey();
			Set<EdgeTargetRecord2> edges = entry.getValue();
			for (EdgeTargetRecord2 edge : edges) {
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
	public IVertexPropertyRecord getProperty(final String propertyKey) {
		if(propertyKey == null || this.properties == null || this.properties.isEmpty()){
			return null;
		}
		for(IVertexPropertyRecord record : this.properties){
			if(Objects.equals(record.getKey(), propertyKey)){
				return record;
			}
		}
		return null;
	}

}
