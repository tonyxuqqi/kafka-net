using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using kafka_tests.Helpers;
using KafkaNet;
using KafkaNet.Common;
using KafkaNet.Protocol;
using NUnit.Framework;

namespace kafka_tests.Unit
{
    [TestFixture]
    [Category("Unit")]
    public class ServerAffinityPartitionSelectorTest
    {
        [Test]
        public void KeepSameLeaderWithDifferentTopicAndKeys()
        {
            Topic topic1 = this.CreateTopic(7, (i) => (i % 7));
            Topic topic2 = this.CreateTopic(8, (i) => (i % 8));

            ServerAffinityPartitionSelector selector = new ServerAffinityPartitionSelector();

            Partition partition = selector.Select(topic1, new byte[] { 1 });
            for(int i = 0; i < 32; i++)
            {
                Partition partition2 = selector.Select(topic1, new byte[] { (byte)i });
                Assert.AreEqual(partition.LeaderId, partition2.LeaderId);

                partition2 = selector.Select(topic2, new byte[] { (byte)i });
                Assert.AreEqual(partition.LeaderId, partition2.LeaderId);
            }
        }

        [Test]
        public void UseDifferentLeaderWhenCurrentLeaderDoesNotHaveGivenTopic()
        {
            Topic topic1 = this.CreateTopic(1, (i) => 0);
            Topic topic2 = this.CreateTopic(8, (i) => (i % 8) + 1);

            ServerAffinityPartitionSelector selector = new ServerAffinityPartitionSelector();
            Partition partition = selector.Select(topic1, new byte[] { 1 });
            Assert.AreEqual(partition.LeaderId, 0);

            partition = selector.Select(topic2, new byte[] { 1 });
            Assert.AreNotEqual(partition.LeaderId, 0);

            int leaderId = partition.LeaderId;
            for(int i = 0; i < 32; i++)
            {
                Partition partition2 = selector.Select(topic2, new byte[] { (byte)i });
                Assert.AreEqual(partition2.LeaderId, leaderId);
            }

            partition = selector.Select(topic1, new byte[] { 2 });
            Assert.AreEqual(partition.LeaderId, 0);
        }

        private Topic CreateTopic(int leaderCount, Func<int, int> leaderIdGen)
        {
            List<Partition> partitions = new List<Partition>();
            for (int i = 0; i < 32; i++)
            {
                partitions.Add(new Partition() { LeaderId = leaderIdGen(i), PartitionId = i });
            }

            Topic topic = new Topic()
            {
                Partitions = partitions
            };
            topic.GenerateLeaderPartitionMapFromPartitionList();
            Assert.AreEqual(topic.LeaderPartitionMap.Count, leaderCount);

            return topic;
        }
    }
}
