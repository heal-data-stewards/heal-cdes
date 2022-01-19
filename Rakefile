META_CSV = "HEAL CDEs matching LOINC, NIH CDE or caDSR CDEs - HEAL CDEs mapped to LOINC and caDSR - 2021sep7.csv"
INPUT_FILES = FileList['output/**/*.json']
OUTPUT_FILES = INPUT_FILES.pathmap("%{^output/,annotate/}X.complete")

directory "annotate"

task :default do |dt|
  INPUT_FILES.each do |input_file|
    output_file = input_file.pathmap("%{^output/,annotate/}X")
    complete_file = input_file.pathmap("%{^output/,annotate/}X.complete")

    if File.file?(complete_file)
      puts "Skipping #{input_file}, job completed"
    else
      puts "Converting #{input_file} to #{output_file}"
      sh 'pipenv', 'run', 'python', 'annotators/scigraph/scigraph-api-annotator.py',
        input_file, META_CSV,
        '--to-kgx', output_file
      sh 'touch', complete_file
    end
  end
end
