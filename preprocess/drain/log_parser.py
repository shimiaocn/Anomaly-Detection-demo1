import regex as re
import pandas as pd
from drain3.file_persistence import FilePersistence
from drain3 import TemplateMiner
from collections import OrderedDict


def generate_logformat_regex(logformat):
    """Function to generate regular expression to split log messages"""
    headers = []
    splitters = re.split(r"(<[^<>]+>)", logformat)
    regex = ""
    for k in range(len(splitters)):
        if k % 2 == 0:
            splitter = re.sub(" +", "\\\s+", splitters[k])
            regex += splitter
        else:
            header = splitters[k].strip("<").strip(">")
            regex += "(?P<%s>.*?)" % header
            headers.append(header)
    regex = re.compile("^" + regex + "$")
    return headers, regex


def log_to_dataframe(log_file, regex, headers):
    """Function to transform log file to dataframe"""
    log_messages = []
    linecount = 0
    with open(log_file, "r") as fin:
        for line in fin.readlines():
            try:
                match = regex.search(line.strip())
                message = [match.group(header) for header in headers]
                log_messages.append(message)
                linecount += 1
            except Exception as e:
                print("[Warning] Skip line: " + line)
    log_df = pd.DataFrame(log_messages, columns=headers)
    log_df.insert(0, "LineId", None)
    log_df["LineId"] = [i + 1 for i in range(linecount)]
    print("Total lines: ", len(log_df))
    return log_df


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
    data_df.to_csv("../../Data/train/HDFS_sequence.csv", index=False)


hdfs_log_file = '../../Data/train/Train_raw.log'  # The input log file name
hdfs_log_format = '<Date> <Time> <Pid> <Level> <Component>: <Content>'  # HDFS log format

df_headers, log_line_regex = generate_logformat_regex(hdfs_log_format)
log_df = log_to_dataframe(hdfs_log_file, log_line_regex, df_headers)

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

log_df.to_csv("../../Data/train/Train_structured.csv", index=False)

hdfs_sampling("../../Data/train/Train_structured.csv")
