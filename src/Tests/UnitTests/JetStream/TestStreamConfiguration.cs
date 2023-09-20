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
        private StreamConfiguration GetTestConfiguration() {
            string json = ReadDataFile("StreamConfiguration.json");
            return new StreamConfiguration(json);
        }

        [Fact]
        public void TestConstruction()
        {
            StreamConfiguration testSc = GetTestConfiguration();
            // from json
            Validate(testSc, false, CompressionOption.S2);

            // Validate(new StreamConfiguration(testSc.ToJsonNode()));
            Validate(new StreamConfiguration(testSc.ToJsonNode().ToString()), false, CompressionOption.S2);

            StreamConfiguration.StreamConfigurationBuilder builder = StreamConfiguration.Builder(testSc);
            Validate(builder.Build(), false, CompressionOption.S2);
            
            Dictionary<string, string> metadata = new Dictionary<string, string>(); metadata["meta-foo"] = "meta-bar";

            builder.WithName(testSc.Name)
                    .WithSubjects(testSc.Subjects)
                    .WithRetentionPolicy(testSc.RetentionPolicy)
                    .WithCompressionOption(testSc.CompressionOption)
                    .WithMaxConsumers(testSc.MaxConsumers)
                    .WithMaxMessages(testSc.MaxMsgs)
                    .WithMaxMessagesPerSubject(testSc.MaxMsgsPerSubject)
                    .WithMaxBytes(testSc.MaxBytes)
                    .WithMaxAge(testSc.MaxAge)
                    .WithMaxMsgSize(testSc.MaxMsgSize)
                    .WithStorageType(testSc.StorageType)
                    .WithReplicas(testSc.Replicas)
                    .WithNoAck(testSc.NoAck)
                    .WithTemplateOwner(testSc.TemplateOwner)
                    .WithDiscardPolicy(testSc.DiscardPolicy)
                    .WithDuplicateWindow(testSc.DuplicateWindow)
                    .WithPlacement(testSc.Placement)
                    .WithMirror(testSc.Mirror)
                    .WithSources(testSc.Sources)
                    .WithAllowRollup(testSc.AllowRollup)
                    .WithAllowDirect(testSc.AllowDirect)
                    .WithMirrorDirect(testSc.MirrorDirect)
                    .WithDenyDelete(testSc.DenyDelete)
                    .WithDenyPurge(testSc.DenyPurge)
                    .WithDiscardNewPerSubject(testSc.DiscardNewPerSubject)
                    .WithMetadata(metadata)
                    .WithFirstSequence(82942U)
                ;
            Validate(builder.Build(), false, CompressionOption.S2);
            Validate(builder.AddSources((Source)null).Build(), false, CompressionOption.S2);

            List<Source> sources = new List<Source>(testSc.Sources);
            sources.Add(null);
            Source copy = new Source(sources[0].ToJsonNode());
            sources.Add(copy);
            Validate(builder.AddSources(sources).Build(), false, CompressionOption.S2);
            
            // covering add a single source
            sources = new List<Source>(testSc.Sources);
            builder.WithSources(new List<Source>()); // clears the sources
            builder.AddSource(null); // coverage
            foreach (Source source in sources) {
                builder.AddSource(source);
            }
            builder.AddSource(sources[0]);
            Validate(builder.Build(), false, CompressionOption.S2);
        }

        [Fact]
        public void TestConstructionInvalidsCoverage()
        {
            Assert.Throws<ArgumentException>(() => StreamConfiguration.Builder().WithName(HasSpace));
            Assert.Throws<ArgumentException>(() => StreamConfiguration.Builder().WithMaxConsumers(0));
            Assert.Throws<ArgumentException>(() => StreamConfiguration.Builder().WithMaxConsumers(-2));
            Assert.Throws<ArgumentException>(() => StreamConfiguration.Builder().WithMaxMessages(0));
            Assert.Throws<ArgumentException>(() => StreamConfiguration.Builder().WithMaxMessages(-2));
            Assert.Throws<ArgumentException>(() => StreamConfiguration.Builder().WithMaxMessagesPerSubject(0));
            Assert.Throws<ArgumentException>(() => StreamConfiguration.Builder().WithMaxMessagesPerSubject(-2));
            Assert.Throws<ArgumentException>(() => StreamConfiguration.Builder().WithMaxBytes(0));
            Assert.Throws<ArgumentException>(() => StreamConfiguration.Builder().WithMaxBytes(-2));
            Assert.Throws<ArgumentException>(() => StreamConfiguration.Builder().WithMaxAge(Duration.OfNanos(-1)));
            Assert.Throws<ArgumentException>(() => StreamConfiguration.Builder().WithMaxAge(-1));
            Assert.Throws<ArgumentException>(() => StreamConfiguration.Builder().WithMaxMsgSize(0));
            Assert.Throws<ArgumentException>(() => StreamConfiguration.Builder().WithMaxMsgSize(-2));
            Assert.Throws<ArgumentException>(() => StreamConfiguration.Builder().WithReplicas(0));
            Assert.Throws<ArgumentException>(() => StreamConfiguration.Builder().WithReplicas(6));
            Assert.Throws<ArgumentException>(() => StreamConfiguration.Builder().WithDuplicateWindow(Duration.OfNanos(-1)));
            Assert.Throws<ArgumentException>(() => StreamConfiguration.Builder().WithDuplicateWindow(-1));        
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
            List<string> list45 = new List<string>();
            list45.Add(Subject(4));
            list45.Add(Subject(5));
            builder.WithSubjects(list45);
            AssertSubjects(builder.Build(), 4, 5);

            // AddSubjects(...) adds unique
            builder.AddSubjects(Subject(5), Subject(6));
            AssertSubjects(builder.Build(), 4, 5, 6);

            // AddSubjects(...) adds unique
            List<string> list678 = new List<string>();
            list678.Add(Subject(6));
            list678.Add(Subject(7));
            list678.Add(Subject(8));
            builder.AddSubjects(list678);
            AssertSubjects(builder.Build(), 4, 5, 6, 7, 8);

            // AddSubjects(...) null check
            builder.AddSubjects((string[]) null);
            AssertSubjects(builder.Build(), 4, 5, 6, 7, 8);

            // AddSubjects(...) null check
            builder.AddSubjects((List<string>) null);
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

            builder.WithRetentionPolicy(RetentionPolicy.Limits);
            Assert.Equal(RetentionPolicy.Limits, builder.Build().RetentionPolicy);

            builder.WithRetentionPolicy(null);
            Assert.Equal(RetentionPolicy.Limits, builder.Build().RetentionPolicy);

            builder.WithRetentionPolicy(RetentionPolicy.Interest);
            Assert.Equal(RetentionPolicy.Interest, builder.Build().RetentionPolicy);

            builder.WithRetentionPolicy(RetentionPolicy.WorkQueue);
            Assert.Equal(RetentionPolicy.WorkQueue, builder.Build().RetentionPolicy);
        }

        [Fact]
        public void TestCompressionOption()
        {
            StreamConfiguration.StreamConfigurationBuilder builder = StreamConfiguration.Builder();
            Assert.Equal(CompressionOption.None, builder.Build().CompressionOption);

            builder.WithCompressionOption(CompressionOption.None);
            Assert.Equal(CompressionOption.None, builder.Build().CompressionOption);

            builder.WithCompressionOption(null);
            Assert.Equal(CompressionOption.None, builder.Build().CompressionOption);
            Assert.DoesNotContain("\"compression\"", builder.Build().ToJsonString());

            builder.WithCompressionOption(CompressionOption.S2);
            Assert.Equal(CompressionOption.S2, builder.Build().CompressionOption);
            Assert.Contains("\"compression\":\"s2\"", builder.Build().ToJsonString());
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

        private void Validate(StreamConfiguration sc, bool serverTest, CompressionOption compressionOption)
        {
            {
                Assert.Equal("sname", sc.Name);
                Assert.Equal("blah blah", sc.Description);
                Assert.Collection(sc.Subjects,
                    item => item.Equals("foo"),
                    item => item.Equals("bar"),
                    item => item.Equals("repub.>"));

                Assert.Equal(compressionOption, sc.CompressionOption);

                Assert.Equal(RetentionPolicy.Interest, sc.RetentionPolicy);
                Assert.Equal(730, sc.MaxConsumers);
                Assert.Equal(731, sc.MaxMsgs);
                Assert.Equal(741, sc.MaxMsgsPerSubject);
                Assert.Equal(732, sc.MaxBytes);
                Assert.Equal(Duration.OfNanos(43000000000L), sc.MaxAge);
                Assert.Equal(Duration.OfNanos(42000000000L), sc.DuplicateWindow);
                Assert.Equal(734, sc.MaxMsgSize);
                Assert.Equal(StorageType.Memory, sc.StorageType);
                Assert.Equal(DiscardPolicy.New, sc.DiscardPolicy);

                Assert.NotNull(sc.Placement);
                Assert.Equal("clstr", sc.Placement.Cluster);
                Assert.Collection(sc.Placement.Tags, item => item.Equals("tag1"), item => item.Equals("tag2"));

                Assert.NotNull(sc.Republish);
                Assert.Equal("repub.>", sc.Republish.Source);
                Assert.Equal("dest.>", sc.Republish.Destination);
                Assert.True(sc.Republish.HeadersOnly);

                DateTime zdt = AsDateTime("2020-11-05T19:33:21.163377Z");

                if (serverTest)
                {
                    Assert.Equal(1, sc.Replicas);
                }
                else
                {
                    Assert.True(sc.NoAck);
                    Assert.True(sc.Sealed);
                    Assert.True(sc.DenyDelete);
                    Assert.True(sc.DenyPurge);
                    Assert.True(sc.DiscardNewPerSubject);
                    Assert.True(sc.AllowRollup);
                    Assert.True(sc.AllowDirect);
                    Assert.True(sc.MirrorDirect);

                    Assert.Equal(5, sc.Replicas);
                    Assert.Equal("twnr", sc.TemplateOwner);
                    Assert.NotNull(sc.Mirror);
                    Assert.Equal("eman", sc.Mirror.Name);
                    Assert.Equal(736U, sc.Mirror.StartSeq);
                    Assert.Equal(zdt, sc.Mirror.StartTime);
                    Assert.Equal("mfsub", sc.Mirror.FilterSubject);

                    Assert.Equal(2, sc.Sources.Count);
                    Assert.Collection(sc.Sources,
                        item => ValidateSource(item, "s0", 737, "s0sub", "s0api", "s0dlvrsub", zdt),
                        item => ValidateSource(item, "s1", 738, "s1sub", "s1api", "s1dlvrsub", zdt));

                    Assert.Single(sc.Metadata);
                    Assert.Equal("meta-bar", sc.Metadata["meta-foo"]);
                    Assert.Equal(82942U, sc.FirstSequence);

                }
            }
        }

        private void ValidateSource(Source source, string name, ulong seq, string filter, string api, string deliver, DateTime zdt)
        {
            Assert.Equal(name, source.Name);
            Assert.Equal(seq, source.StartSeq);
            Assert.Equal(zdt, source.StartTime);
            Assert.Equal(filter, source.FilterSubject);

            Assert.NotNull(source.External);
            Assert.Equal(api, source.External.Api);
            Assert.Equal(deliver, source.External.Deliver);
        }

        [Fact]
        public void TestPlacement() {
            Assert.Throws<ArgumentException>(() => Placement.Builder().Build());

            Placement p = Placement.Builder().WithCluster("cluster").Build();
            Assert.Equal("cluster", p.Cluster);
            Assert.Null(p.Tags);

            List<string> tags = new List<string> { "a", "b" };
            p = Placement.Builder().WithCluster("cluster").WithTags(tags).Build();
            Assert.Equal("cluster", p.Cluster);
            Assert.Equal(2, p.Tags.Count);
        }

        [Fact]
        public void TestExternal()
        {
            External e = new External("api1", "deliver1");
            Assert.Equal("api1", e.Api);
            Assert.Equal("deliver1", e.Deliver);

            e = External.Builder().WithApi("api2").WithDeliver("deliver2").Build();
            Assert.Equal("api2", e.Api);
            Assert.Equal("deliver2", e.Deliver);
        }

        [Fact]
        public void TestSource()
        {
            DateTime now = DateTime.UtcNow;
            Source s = new Source("name1", 1, now, "fs1", new External("api1", "deliver1"));
            Assert.Equal("name1", s.Name);
            Assert.Equal(1U, s.StartSeq);
            Assert.Equal(now, s.StartTime);
            Assert.Equal("fs1", s.FilterSubject);
            Assert.Equal("api1", s.External.Api);
            Assert.Equal("deliver1", s.External.Deliver);
            
            now = DateTime.UtcNow;
            s = Source.Builder()
                .WithName("name2")
                .WithStartSeq(2)
                .WithStartTime(now)
                .WithFilterSubject("fs2")
                .WithExternal(new External("api2", "deliver2"))
                .Build();
            Assert.Equal("name2", s.Name);
            Assert.Equal(2U, s.StartSeq);
            Assert.Equal(now, s.StartTime);
            Assert.Equal("fs2", s.FilterSubject);
            Assert.Equal("api2", s.External.Api);
            Assert.Equal("deliver2", s.External.Deliver);

            string json = ReadDataFile("MirrorsSources.json");
            List<Source> slist = Source.OptionalListOf(JSONNode.Parse(json));
            Assert.Equal(5, slist.Count);
            
            string[] lines = SplitLines(json);
            foreach (string l1 in lines) {
                if (l1.StartsWith("{")) {
                    Mirror m1 = new Mirror(JSON.Parse(l1));
                    Assert.Equal(m1, m1);
                    Assert.Equal(m1, Mirror.Builder(m1).Build());
                    Source s1 = new Source(JSON.Parse(l1));
                    Assert.Equal(s1, s1);
                    Assert.Equal(s1, Source.Builder(s1).Build());
                    Assert.NotEqual(m1, new Object());
                    foreach (string l2 in lines) {
                        if (l2.StartsWith("{")) {
                            Mirror m2 = new Mirror(JSON.Parse(l2));
                            Source s2 = new Source(JSON.Parse(l2));
                            if (l1.Equals(l2)) {
                                Assert.Equal(m1, m2);
                                Assert.Equal(s1, s2);
                            }
                            else {
                                Assert.NotEqual(m1, m2);
                                Assert.NotEqual(s1, s2);
                            }
                        }
                    }
                }
            }        
        }

        [Fact]
        public void TestMirror()
        {
            DateTime now = DateTime.UtcNow;
            Mirror m = new Mirror("name1", 1, now, "fs1", new External("api1", "deliver1"));
            Assert.Equal("name1", m.Name);
            Assert.Equal(1U, m.StartSeq);
            Assert.Equal(now, m.StartTime);
            Assert.Equal("fs1", m.FilterSubject);
            Assert.Equal("api1", m.External.Api);
            Assert.Equal("deliver1", m.External.Deliver);
            
            now = DateTime.UtcNow;
            m = Mirror.Builder()
                .WithName("name2")
                .WithStartSeq(2)
                .WithStartTime(now)
                .WithFilterSubject("fs2")
                .WithExternal(new External("api2", "deliver2"))
                .Build();
            Assert.Equal("name2", m.Name);
            Assert.Equal(2U, m.StartSeq);
            Assert.Equal(now, m.StartTime);
            Assert.Equal("fs2", m.FilterSubject);
            Assert.Equal("api2", m.External.Api);
            Assert.Equal("deliver2", m.External.Deliver);

            string[] lines = ReadDataFileLines("MirrorsSources.json");
            ulong u = 0;
            foreach (string l1 in lines)
            {
                if (l1.StartsWith("{"))
                {
                    u++;
                    Mirror m1 = Mirror.OptionalInstance(JSON.Parse(l1));
                    Assert.Equal(m1, m1);
                    Assert.NotEqual(m1, (Mirror)null);
                    Assert.NotEqual(m1, new Object());
                    Assert.Equal("n" + u, m1.Name);
                    Assert.Equal(u, m1.StartSeq);
                    Assert.Equal("fs" + u, m1.FilterSubject);
                    Assert.Equal("a" + u, m1.External.Api);
                    Assert.Equal("d" + u, m1.External.Deliver);

                    foreach (string l2 in lines)
                    {
                        if (l2.StartsWith("{"))
                        {
                            Mirror m2 = Mirror.OptionalInstance(JSON.Parse(l2));
                            if (l1.Equals(l2))
                            {
                                Assert.Equal(m1, m2);
                            }
                            else
                            {
                                Assert.NotEqual(m1, m2);
                            }
                        }
                    }
                }
            }
        }
    }
}
