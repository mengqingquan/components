// ============================================================================
//
// Copyright (C) 2006-2018 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
// ============================================================================
package org.talend.components.localio.runtime.fixed;

import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.IndexedRecord;
import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.nexmark.model.Person;
import org.apache.beam.sdk.nexmark.sources.BoundedEventSource;
import org.apache.beam.sdk.nexmark.sources.UnboundedEventSource;
import org.apache.beam.sdk.nexmark.sources.generator.GeneratorConfig;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;
import org.talend.components.adapter.beam.BeamLocalRunnerOption;
import org.talend.components.adapter.beam.coders.LazyAvroCoder;
import org.talend.components.adapter.beam.transform.DirectConsumerCollector;
import org.talend.components.localio.LocalIOErrorCode;
import org.talend.components.localio.fixed.FixedDatasetProperties;
import org.talend.components.localio.fixed.FixedDatasetProperties.PredefinedType;
import org.talend.components.localio.fixed.FixedInputProperties;
import org.talend.daikon.exception.TalendRuntimeException;
import org.talend.daikon.java8.Consumer;

public class PredefinedDatasets {

    /**
     * All events will be given a timestamp relative to this time (ms since epoch).
     */
    private static final long BASE_TIME = Instant.parse("2015-07-15T00:00:00.000Z").getMillis();

    public static Schema getPredefinedSchema(PredefinedType value) {
        switch (value) {
        case NEXMARK:
            return EventIndexedRecord.SCHEMA;
        }
        throw LocalIOErrorCode.createCannotParseSchema(null, value.name());
    }

    public static PCollection<IndexedRecord> getSourceCollection(PBegin begin, FixedInputProperties properties) {
        boolean isStreaming = properties.isStreaming.getValue();
        switch (properties.getDatasetProperties().predefined.getValue()) {
        case NEXMARK:
            // Use the "reasonable defaults" directly from the options.
            NexmarkConfiguration config = new NexmarkConfiguration();
            config.numEventGenerators = 1;
            if (isStreaming) {
                config.isRateLimited = true;
            } else {
                config.numEvents = properties.repeat.getValue();
            }
            GeneratorConfig genConfig = new GeneratorConfig(config, //
                    config.useWallclockEventTime ? System.currentTimeMillis() : BASE_TIME, //
                    0L, config.numEvents, 0L);

            PCollection<Event> from;
            if (isStreaming) {
                from = begin.apply("NEXMark.unbounded", Read.from(new UnboundedEventSource(genConfig, config.numEventGenerators,
                        config.watermarkHoldbackSec, config.isRateLimited)));
            } else {
                from = begin.apply("NEXMark.bounded", Read.from(new BoundedEventSource(genConfig, config.numEventGenerators)));
            }
            // from = from.apply("NEXMark.filterBids", Filter.by(IS_BID));

            return from.apply("NEXMark.mapToAvro", MapElements.via(new SimpleFunction<Event, IndexedRecord>() {

                public EventIndexedRecord apply(Event e) {
                    return new EventIndexedRecord(e);
                }
            })).setCoder(LazyAvroCoder.of());

        }
        throw LocalIOErrorCode.createCannotParseSchema(null, properties.getDatasetProperties().predefined.getValue().name());
    }

    public static void getSample(final List<IndexedRecord> values, FixedDatasetProperties properties, int limit) {

        DirectOptions options = BeamLocalRunnerOption.getOptions();
        final Pipeline p = Pipeline.create(options);

        FixedInputProperties inputProperties = new FixedInputProperties(null);
        inputProperties.setDatasetProperties(properties);
        inputProperties.init();

        inputProperties.isStreaming.setValue(false);
        inputProperties.repeat.setValue(limit);

        try (DirectConsumerCollector<IndexedRecord> collector = DirectConsumerCollector.of(new Consumer<IndexedRecord>() {

            @Override
            public void accept(IndexedRecord r) {
                values.add(r);
            }
        })) {
            // Collect a sample of the input records.
            getSourceCollection(p.begin(), inputProperties).apply(collector);
            try {
                p.run().waitUntilFinish();
            } catch (Pipeline.PipelineExecutionException e) {
                if (e.getCause() instanceof TalendRuntimeException)
                    throw (TalendRuntimeException) e.getCause();
                throw e;
            }
        }

    }

    /**
     * Internal {@link IndexedRecord} representation of a {@link Auction}.
     */
    private static class AuctionIndexedRecord implements IndexedRecord {

        public static Schema SCHEMA = SchemaBuilder.record("Auction").namespace("org.talend.datastreams.nexmark").fields() //
                .requiredLong("id") //
                .requiredString("itemName") //
                .requiredString("description") //
                .requiredLong("initialBid") //
                .requiredLong("reserve") //
                .requiredLong("dateTime") // TODO: timestamp-millis
                .requiredLong("expires") // TODO: timestamp-millis
                .requiredLong("seller") //
                .requiredLong("category") //
                .optionalString("extra").endRecord();

        private final Auction auction;

        public AuctionIndexedRecord(Auction auction) {
            this.auction = auction;
        }

        @Override
        public Schema getSchema() {
            return SCHEMA;
        }

        @Override
        public Object get(int i) {
            switch (i) {
            case 0:
                return auction.id;
            case 1:
                return auction.itemName;
            case 2:
                return auction.description;
            case 3:
                return auction.initialBid;
            case 4:
                return auction.reserve;
            case 5:
                return auction.dateTime;
            case 6:
                return auction.expires;
            case 7:
                return auction.seller;
            case 8:
                return auction.category;
            case 9:
                return auction.extra;
            }
            return null;
        }

        @Override
        public void put(int i, Object v) {
            throw new RuntimeException("Read-only record " + getClass().getSimpleName());
        }
    }

    /**
     * Internal {@link IndexedRecord} representation of a {@link Bid}.
     */
    private static class BidIndexedRecord implements IndexedRecord {

        public static Schema SCHEMA = SchemaBuilder.record("Bid").namespace("org.talend.datastreams.nexmark").fields() //
                .requiredLong("auction") //
                .requiredLong("bidder") //
                .requiredLong("price") //
                .requiredLong("dateTime") // TODO: timestamp-millis
                .optionalString("extra").endRecord();

        private final Bid bid;

        public BidIndexedRecord(Bid bid) {
            this.bid = bid;
        }

        @Override
        public Schema getSchema() {
            return SCHEMA;
        }

        @Override
        public Object get(int i) {
            switch (i) {
            case 0:
                return bid.auction;
            case 1:
                return bid.bidder;
            case 2:
                return bid.price;
            case 3:
                return bid.dateTime;
            case 4:
                return bid.extra;
            }
            return null;
        }

        @Override
        public void put(int i, Object v) {
            throw new RuntimeException("Read-only record " + getClass().getSimpleName());
        }
    }

    /**
     * Internal {@link IndexedRecord} representation of a {@link Person}.
     */
    private static class PersonIndexedRecord implements IndexedRecord {

        public static Schema SCHEMA = SchemaBuilder.record("Person").namespace("org.talend.datastreams.nexmark").fields() //
                .requiredLong("id") //
                .requiredString("name") //
                .requiredString("emailAddress") //
                .requiredString("creditCard") //
                .requiredString("city") //
                .requiredString("state") //
                .requiredLong("dateTime") // TODO: timestamp-millis
                .optionalString("extra").endRecord();

        private final Person person;

        public PersonIndexedRecord(Person person) {
            this.person = person;
        }

        @Override
        public Schema getSchema() {
            return SCHEMA;
        }

        @Override
        public Object get(int i) {
            switch (i) {
            case 0:
                return person.id;
            case 1:
                return person.name;
            case 2:
                return person.emailAddress;
            case 3:
                return person.creditCard;
            case 4:
                return person.city;
            case 5:
                return person.state;
            case 6:
                return person.dateTime;
            case 7:
                return person.extra;
            }
            return null;
        }

        @Override
        public void put(int i, Object v) {
            throw new RuntimeException("Read-only record " + getClass().getSimpleName());
        }
    }

    /**
     * Internal {@link IndexedRecord} representation of a {@link Bid}.
     */
    private static class EventIndexedRecord implements IndexedRecord {

        public static Schema SCHEMA = SchemaBuilder.record("Event").namespace("org.talend.datastreams.nexmark").fields() //
                .name("auction").type(Schema.createUnion(Schema.create(Schema.Type.NULL), AuctionIndexedRecord.SCHEMA))
                .noDefault() //
                .name("bid").type(Schema.createUnion(Schema.create(Schema.Type.NULL), BidIndexedRecord.SCHEMA)).noDefault() //
                .name("person").type(Schema.createUnion(Schema.create(Schema.Type.NULL), PersonIndexedRecord.SCHEMA)).noDefault() //
                .endRecord();

        private final Event event;

        public EventIndexedRecord(Event event) {
            this.event = event;
        }

        @Override
        public Schema getSchema() {
            return SCHEMA;
        }

        @Override
        public Object get(int i) {
            switch (i) {
            case 0:
                if (event.newAuction != null)
                    return new AuctionIndexedRecord(event.newAuction);
                return null;
            case 1:
                if (event.bid != null)
                    return new BidIndexedRecord(event.bid);
                return null;
            case 2:
                if (event.newPerson != null)
                    return new PersonIndexedRecord(event.newPerson);
                return null;
            }
            return null;
        }

        @Override
        public void put(int i, Object v) {
            throw new RuntimeException("Read-only record " + getClass().getSimpleName());
        }
    }

    /**
     * Internal {@link IndexedRecord} representation of a {@link Bid} as a union of records.  The extra enum is
     * not technically useful, except for easy filtering.
     */
    private static class EventIndexedRecord2 implements IndexedRecord {

        public static Schema TYPE_SCHEMA = SchemaBuilder.enumeration("EventType").namespace("org.talend.datastreams.nexmark")
                .symbols("auction", "bid", "person");

        GenericEnumSymbol AUCTION = new GenericData.EnumSymbol(TYPE_SCHEMA, "auction");

        GenericEnumSymbol BID = new GenericData.EnumSymbol(TYPE_SCHEMA, "bid");

        GenericEnumSymbol PERSON = new GenericData.EnumSymbol(TYPE_SCHEMA, "person");

        public static Schema SCHEMA = SchemaBuilder.record("Event").namespace("org.talend.datastreams.nexmark").fields() //
                .name("event")
                .type(Schema.createUnion(AuctionIndexedRecord.SCHEMA, BidIndexedRecord.SCHEMA, PersonIndexedRecord.SCHEMA))
                .noDefault() //
                .name("type").type().enumeration("EventType").symbols("auction", "bid", "person").noDefault() //
                .endRecord();

        private final Event event;

        public EventIndexedRecord2(Event event) {
            this.event = event;
        }

        @Override
        public Schema getSchema() {
            return SCHEMA;
        }

        @Override
        public Object get(int i) {
            switch (i) {
            case 0:
                if (event.newAuction != null)
                    return new AuctionIndexedRecord(event.newAuction);
                if (event.bid != null)
                    return new BidIndexedRecord(event.bid);
                return new PersonIndexedRecord(event.newPerson);
            case 1:
                if (event.newAuction != null)
                    return AUCTION;
                if (event.bid != null)
                    return BID;
                return PERSON;
            }
            return null;
        }

        @Override
        public void put(int i, Object v) {
            throw new RuntimeException("Read-only record " + getClass().getSimpleName());
        }
    }

}
