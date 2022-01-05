// Copyright 2020 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Collections.Generic;
using NATS.Client.Internals;
using NATS.Client.Internals.SimpleJSON;
using NATS.Client.JetStream;
using Xunit;

namespace UnitTests.JetStream
{
    public class TestStreamConfiguration : TestBase
    {
        private StreamConfiguration getTestConfiguration() {
            String json = ReadDataFile("StreamConfiguration.json");
            return new StreamConfiguration(json);
        }

        [Fact]
        public void BuilderWorks()
        {
        }

        [Fact]
        public void TestConstruction()
        {
            StreamConfiguration testSc = getTestConfiguration();
            // from json
            Validate(testSc);

            // Validate(new StreamConfiguration(testSc.ToJsonNode()));
            Validate(new StreamConfiguration(testSc.ToJsonNode().ToString()));

            StreamConfiguration.StreamConfigurationBuilder builder = StreamConfiguration.Builder(testSc);
            Validate(builder.Build());
            
            builder.WithName(testSc.Name)
                    .WithSubjects(testSc.Subjects)
                    .WithRetentionPolicy(testSc.RetentionPolicy)
                    .WithMaxConsumers(testSc.MaxConsumers)
                    .WithMaxMessages(testSc.MaxMsgs)
                    .WithMaxMessagesPerSubject(testSc.MaxMsgsPerSubject)
                    .WithMaxBytes(testSc.MaxBytes)
                    .WithMaxAge(testSc.MaxAge)
                    .WithMaxMsgSize(testSc.MaxValueSize)
                    .WithStorageType(testSc.StorageType)
                    .WithReplicas(testSc.Replicas)
                    .WithNoAck(testSc.NoAck)
                    .WithTemplateOwner(testSc.TemplateOwner)
                    .WithDiscardPolicy(testSc.DiscardPolicy)
                    .WithDuplicateWindow(testSc.DuplicateWindow)
                    .WithPlacement(testSc.Placement)
                    .WithMirror(testSc.Mirror)
                    .WithSources(testSc.Sources)
                ;
            Validate(builder.Build());
            Validate(builder.AddSources((Source)null).Build());

            List<Source> sources = new List<Source>(testSc.Sources);
            sources.Add(null);
            Source copy = new Source(sources[0].ToJsonNode());
            sources.Add(copy);
            Validate(builder.AddSources(sources).Build());
        }

        [Fact]
        public void TestSourceBaseEquals()
        {
            string[] lines = ReadDataFileLines("SourceBaseJson.txt");
            foreach (string l1 in lines)
            {
                Mirror m1 = NATS.Client.JetStream.Mirror.OptionalInstance(JSON.Parse(l1));
                Assert.Equal(m1, m1);
                Assert.NotEqual(m1, (Mirror)null);
                Assert.NotEqual(m1, new Object());
                foreach (string l2 in lines) {
                    Mirror m2 = NATS.Client.JetStream.Mirror.OptionalInstance(JSON.Parse(l2));
                    if (l1.Equals(l2)) {
                        Assert.Equal(m1, m2);
                    }
                    else {
                        Assert.NotEqual(m1, m2);
                    }
                }
            }
        }

        [Fact]
        public void TestExternalEquals()
        {
            string[] lines = ReadDataFileLines("ExternalJson.txt");
            foreach (string l1 in lines)
            {
                External e1 = External.OptionalInstance(JSON.Parse(l1));
                Assert.Equal(e1, e1);
                Assert.NotEqual(e1, (External)null);
                Assert.NotEqual(e1, new Object());
                foreach (string l2 in lines) {
                    External e2 = External.OptionalInstance(JSON.Parse(l2));
                    if (l1.Equals(l2)) {
                        Assert.Equal(e1, e2);
                    }
                    else {
                        Assert.NotEqual(e1, e2);
                    }
                }
            }
        }

        [Fact]
        public void TestSubjects() {
            StreamConfiguration.StreamConfigurationBuilder builder = StreamConfiguration.Builder();

            // subjects(...) replaces
            builder.WithSubjects(Subject(0));
            AssertSubjects(builder.Build(), 0);

            // subjects(...) replaces
            builder.WithSubjects();
            AssertSubjects(builder.Build());

            // subjects(...) replaces
            builder.WithSubjects(Subject(1));
            AssertSubjects(builder.Build(), 1);

            // Subjects(...) replaces
            builder.WithSubjects((string)null);
            AssertSubjects(builder.Build());

            // Subjects(...) replaces
            builder.WithSubjects(Subject(2), Subject(3));
            AssertSubjects(builder.Build(), 2, 3);

            // Subjects(...) replaces
            builder.WithSubjects(Subject(101), null, Subject(102));
            AssertSubjects(builder.Build(), 101, 102);

            // Subjects(...) replaces
            List<String> list45 = new List<String>();
            list45.Add(Subject(4));
            list45.Add(Subject(5));
            builder.WithSubjects(list45);
            AssertSubjects(builder.Build(), 4, 5);

            // AddSubjects(...) adds unique
            builder.AddSubjects(Subject(5), Subject(6));
            AssertSubjects(builder.Build(), 4, 5, 6);

            // AddSubjects(...) adds unique
            List<String> list678 = new List<String>();
            list678.Add(Subject(6));
            list678.Add(Subject(7));
            list678.Add(Subject(8));
            builder.AddSubjects(list678);
            AssertSubjects(builder.Build(), 4, 5, 6, 7, 8);

            // AddSubjects(...) null check
            builder.AddSubjects((String[]) null);
            AssertSubjects(builder.Build(), 4, 5, 6, 7, 8);

            // AddSubjects(...) null check
            builder.AddSubjects((List<String>) null);
            AssertSubjects(builder.Build(), 4, 5, 6, 7, 8);
        }

        private void AssertSubjects(StreamConfiguration sc, params int[] subIds)
        {
            int count = sc.Subjects.Count;
            Assert.Equal(subIds.Length, count);
            foreach (int subId in subIds) {
                Assert.Contains(Subject(subId), sc.Subjects);
            }
        }

        [Fact]
        public void TestRetentionPolicy()
        {
            StreamConfiguration.StreamConfigurationBuilder builder = StreamConfiguration.Builder();
            Assert.Equal(RetentionPolicy.Limits, builder.Build().RetentionPolicy);

            builder.WithRetentionPolicy(RetentionPolicy.Interest);
            Assert.Equal(RetentionPolicy.Interest, builder.Build().RetentionPolicy);

            builder.WithRetentionPolicy(RetentionPolicy.WorkQueue);
            Assert.Equal(RetentionPolicy.WorkQueue, builder.Build().RetentionPolicy);

            builder.WithRetentionPolicy(null);
            Assert.Equal(RetentionPolicy.Limits, builder.Build().RetentionPolicy);
        }

        [Fact]
        public void TestStorageType() {
            StreamConfiguration.StreamConfigurationBuilder builder = StreamConfiguration.Builder();
            Assert.Equal(StorageType.File, builder.Build().StorageType);

            builder.WithStorageType(StorageType.Memory);
            Assert.Equal(StorageType.Memory, builder.Build().StorageType);

            builder.WithStorageType(null);
            Assert.Equal(StorageType.File, builder.Build().StorageType);
        }

        [Fact]
        public void TestDiscardPolicy() {
            StreamConfiguration.StreamConfigurationBuilder builder = StreamConfiguration.Builder();
            Assert.Equal(DiscardPolicy.Old, builder.Build().DiscardPolicy);

            builder.WithDiscardPolicy(DiscardPolicy.New);
            Assert.Equal(DiscardPolicy.New, builder.Build().DiscardPolicy);

            builder.WithDiscardPolicy(null);
            Assert.Equal(DiscardPolicy.Old, builder.Build().DiscardPolicy);
        }

        private void Validate(StreamConfiguration sc)
        {
            Assert.Equal("sname", sc.Name);
            Assert.Collection(sc.Subjects,
                item => item.Equals("foo"),
                item => item.Equals("bar"));

            Assert.Equal(RetentionPolicy.Interest, sc.RetentionPolicy);
            Assert.Equal(730, sc.MaxConsumers);
            Assert.Equal(731, sc.MaxMsgs);
            Assert.Equal(7311, sc.MaxMsgsPerSubject);
            Assert.Equal(732, sc.MaxBytes);
            Assert.Equal(Duration.OfNanos(42000000000L), sc.MaxAge);
            Assert.Equal(734, sc.MaxValueSize);
            Assert.Equal(StorageType.Memory, sc.StorageType);
            Assert.Equal(5, sc.Replicas);
            Assert.False(sc.NoAck);
            Assert.Equal("twnr", sc.TemplateOwner);
            Assert.Equal(DiscardPolicy.New, sc.DiscardPolicy);
            Assert.Equal(Duration.OfNanos(73000000000L), sc.DuplicateWindow);

            Assert.NotNull(sc.Placement);
            Assert.Equal("clstr", sc.Placement.Cluster);
            Assert.Collection(sc.Placement.Tags, item => item.Equals("tag1"), item => item.Equals("tag2"));

            DateTime zdt = AsDateTime("2020-11-05T19:33:21.163377Z");

            Assert.NotNull(sc.Mirror);
            Assert.Equal("eman", sc.Mirror.Name);
            Assert.Equal(736u, sc.Mirror.StartSeq);
            Assert.Equal(zdt, sc.Mirror.StartTime);
            Assert.Equal("mfsub", sc.Mirror.FilterSubject);

            Assert.NotNull(sc.Mirror.External);
            Assert.Equal("apithing", sc.Mirror.External.Api);
            Assert.Equal("dlvrsub", sc.Mirror.External.Deliver);

            Assert.Equal(2, sc.Sources.Count);
            Assert.Collection(sc.Sources,
                item => ValidateSource(item, "s0", 737, "s0sub", "s0api", "s0dlvrsub", zdt),
                item => ValidateSource(item, "s1", 738, "s1sub", "s1api", "s1dlvrsub", zdt));
        }

        private void ValidateSource(Source source, string name, ulong seq, string filter, string api, string deliver,
            DateTime zdt)
        {
            Assert.Equal(name, source.Name);
            Assert.Equal(seq, source.StartSeq);
            Assert.Equal(zdt, source.StartTime);
            Assert.Equal(filter, source.FilterSubject);

            Assert.NotNull(source.External);
            Assert.Equal(api, source.External.Api);
            Assert.Equal(deliver, source.External.Deliver);
        }
    }
}
