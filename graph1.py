from clover import Graph
from clover import S3ListFiles

# just a straightforward python

g = Graph()

# phases. sequentially.
ph = g.add_phase(100)

# subsequent calls to nodes will add them to the previous phase
# this call returns an edge ?
s3_files_list = S3ListFiles(s3_creds, s3_bucket, s3_path, s3_listing_metadata)

# this call uses previous edge to read files and returns an edge again
s3_files = ReadS3Files(s3_creds, s3_bucket, s3_files_list, s3_files_metadata)

# this call puts file list into temportary storage
# we can't store list of files in a local variable
WriteToFile(filename_to_cleanup, s3_files_list)

# How are we going to call transform 
class MyReformatter:
    def __init__():
        return True

    def transform(i, o1, o2):
        if i.type == 'G':
            o1.name = i.name
            o1.surname = i.name
            return 0
        else:
            o2.name = i.name
            o2.surname = i.name
            return 1



# this is a reformat. It receives a reformatter class interface and a metadata for input
# we need to supply output metadata
s3_files_reformatted, s3_files_reformatted2 = Reformat(MyReformatter, s3_files, s3_files_reformatted_metadata, s3_files_reformatted_metadata2)

# calc hash: supply parameters
# it considers all fields except id and tech fields as measures by default
s3_files_hashed = CalcKeyAndMeasureHash("id", s3_files_reformatted, s3_files_reformatted_metadata)

# load them into RSH. Note there is no output edge
# The component truncates and loads a staging table
# We may to the merging here
LoadIntoRedshift(rsh_connection, stg_table_name, temp_s3_folder, s3_files_hashed)

# ---------------------
# next phase

ph2 = g.add_phase(200)

# run history_keeper
MergeHistory(rsh_connection, target_table, stg_table_name)

# -----------------------
# next phase

ph3 = g.add_phase(300)

# read file list
s3_file_list = ReadFile(filename_to_cleanup, s3_listing_metadata)

# cleanup files loaded from s3
S3MoveFiles(s3_creds, s3_bucket, s3_path, s3_archive_path, s3_file_list)


# -------------------
# run graph

g.run()

# after the graph has completed, we actually may run another graph


# other components like analyze incoming data structure and update target tables.
# but how can we extract metadata from source table
