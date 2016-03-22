using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Linq;
using KafkaNet.Common;
using KafkaNet.Model;
using KafkaNet.Protocol;

namespace KafkaNet
{
    public class ServerAffinityPartitionSelector : DefaultPartitionSelector
    {
        private int currentLeaderId = -1;

        public override Partition Select(Topic topic, byte[] key)
        {
            // if we already connects to a broker node, keep using that
            if (this.currentLeaderId >= 0 && topic.LeaderPartitionMap.ContainsKey(this.currentLeaderId))
            {
                List<Partition> partitions = topic.LeaderPartitionMap[this.currentLeaderId];
                int index = new Random().Next(partitions.Count);
                return partitions[index];
            }
            else
            {
                byte[] randomKey = Guid.NewGuid().ToByteArray();
                Partition partition = base.Select(topic, randomKey); // we don't use key here because we want to use a random machine
                this.currentLeaderId = partition.LeaderId;
                return partition;
            }
        }

        public void Reset()
        {
            currentLeaderId = -1;
        }
    }
}
