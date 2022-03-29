require 'set'

META_CSV = "HEAL CDEs matching LOINC, NIH CDE or caDSR CDEs - HEAL CDEs mapped to LOINC and caDSR - 2021sep7.csv"
INPUT_FILES = FileList['output/**/*.json']
NODES_FILES = INPUT_FILES.pathmap("%{^output/,annotated/}X_nodes.jsonl")
EDGES_FILES = INPUT_FILES.pathmap("%{^output/,annotated/}X_edges.jsonl")
COMPREHENSIVE_FILES = INPUT_FILES.pathmap("%{^output/,annotated/}X_comprehensive.jsonl")
OUTPUT_FILES = INPUT_FILES.pathmap("%{^output/,annotated/}X.complete")
OUTPUT_NODES = "annotated/output_nodes.jsonl"
OUTPUT_EDGES = "annotated/output_edges.jsonl"
OUTPUT_COMPREHENSIVE = "annotated/output_comprehensive.jsonl"

directory "annotated"

task default: [:scigraph, :concat_kgx]

task :scigraph do
  INPUT_FILES.each do |input_file|
    output_file = input_file.pathmap("%{^output/,annotated/}X")
    complete_file = input_file.pathmap("%{^output/,annotated/}X.complete")

    if File.file?(complete_file)
      puts "Skipping #{input_file}, job completed"
    else
      puts "Converting #{input_file} to #{output_file}"
      sh 'python', 'annotators/scigraph/scigraph-api-annotator.py',
        input_file, META_CSV,
        '--to-kgx', output_file
      sh 'touch', complete_file
    end
  end
end

task :concat_kgx do
  # There are duplicates in this file.
  concepts = Set[]
  NODES_FILES.each do |node_file|
    IO.readlines(node_file).each do |line|
      concepts.add(line)
    end
  end
  File.open(OUTPUT_NODES, "w") do |output|
    concepts.each do |line|
      output.write(line)
    end
  end

   # Shouldn't be any duplicates in these files.
   File.open(OUTPUT_EDGES, "w") do |output|
    EDGES_FILES.each do |edges_file|
      IO.readlines(edges_file).each do |line|
        output.write(line)
      end
    end
   end
   File.open(OUTPUT_COMPREHENSIVE, "w") do |output|
     COMPREHENSIVE_FILES.each do |comprehensive_files|
       IO.readlines(comprehensive_files).each do |line|
         output.write(line + "\n")
       end
     end
   end
end
