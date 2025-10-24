import apache_beam as beam
import re

input_pattern = 'data/'
output_prefix_ham = 'output/fullcodeham'
output_prefix_spam = 'output/fullcodespam'

# Ham Word Count
with beam.Pipeline() as pipeline1:
  ham_pl = (
    pipeline1
      | 'Take in Dataset' >> beam.io.ReadFromText(input_pattern)
      | 'Separate to list' >> beam.Map(lambda line: line.split("\t"))
      | 'Keep only ham' >> beam.Filter(lambda line: line[0] == "ham")
      | 'Find words' >> beam.FlatMap(lambda line: re.findall(r"[a-zA-Z']+", line[1]))
      | 'Pair words with 1' >> beam.Map(lambda word: (word, 1))
      | 'Group and sum' >> beam.CombinePerKey(sum)
      | 'Format results' >> beam.Map(lambda x: x[0]+","+str(x[1]))
      | 'Write results' >> beam.io.WriteToText(output_prefix_ham, file_name_suffix = ".txt")
  )

# Spam Word Count
with beam.Pipeline() as pipeline2:
  spam = (
    pipeline2
      | 'Take in Dataset' >> beam.io.ReadFromText(input_pattern)
      | 'Separate to list' >> beam.Map(lambda line: line.split("\t"))
      | 'Keep only spam' >> beam.Filter(lambda line: line[0] == "spam")
      | 'Find words' >> beam.FlatMap(lambda line: re.findall(r"[a-zA-Z']+", line[1]))
      | 'Pair words with 1' >> beam.Map(lambda word: (word, 1))
      | 'Group and sum' >> beam.CombinePerKey(sum)
      #| 'Format results' >> beam.Map(lambda word_c: str(word_c))
      | 'Format results' >> beam.Map(lambda x: x[0]+","+str(x[1]))
      | 'Write results' >> beam.io.WriteToText(output_prefix_spam, file_name_suffix = ".txt")
  )

