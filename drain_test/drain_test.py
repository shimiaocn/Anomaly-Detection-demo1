from collections import OrderedDict
import regex as re
import pandas as pd
from drain3.file_persistence import FilePersistence
from drain3 import TemplateMiner

def hdfs_sampling(structured_log_file, window='session'):
    assert window == 'session', "Only window=session is supported for HDFS dataset."
    print("Loading", structured_log_file)
    struct_log = pd.read_csv(structured_log_file, engine='c',
                             na_filter=False, memory_map=True)
    data_dict = OrderedDict()
    for idx, row in struct_log.iterrows():
        blkId_list = re.findall(r'(blk_-?\d+)', row['Content'])
        blkId_set = set(blkId_list)
        for blk_Id in blkId_set:
            if not blk_Id in data_dict:
                data_dict[blk_Id] = []
            data_dict[blk_Id].append(row['EventId'])
    data_df = pd.DataFrame(list(data_dict.items()), columns=['BlockId', 'EventSequence'])
    data_df.to_csv("./HDFS_sequence.csv", index=False)

log_df = pd.read_csv('HDFS_100k.log_structured.csv')
# Drain3 Training Mode
content_list = log_df["Content"].tolist()
persistence = FilePersistence("drain3_state.bin")
template_miner = TemplateMiner(persistence)
for content in content_list:
    result = template_miner.add_log_message(content)
    if result is not None:
        print(f"Template '{result['template_mined']}' added to cluster {result['cluster_id']}")

# Drain3 match Mode
template_list = len(content_list) * [0]
event_id_list = len(content_list) * [0]
for i, content in enumerate(content_list):
    match_result = template_miner.match(content)
    event_id_list[i] = match_result.cluster_id
    template_list[i] = match_result.get_template()

log_df['EventId'] = event_id_list
log_df['Template'] = template_list

template_miner.drain.print_tree()

hdfs_sampling('./HDFS_100k.log_structured.csv')