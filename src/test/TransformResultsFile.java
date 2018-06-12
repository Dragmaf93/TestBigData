package test;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import util.JoinFileUtil;

public class TransformResultsFile {
	
	//Class d'utilità, la utilizzo per fare la proiezione sugli attributi che utilizzerò 
	static JoinFileUtil fileUtil;
	
	//Nuovo tipo di chiave, formata dal solver e dal tempo impiegato, viene utilizzata dal primo Mapper e dal primo Reducer
	public static class KeySolverTime implements WritableComparable<KeySolverTime>
	{

		private String solver;
		private Double time;
		public KeySolverTime() {
			solver="";
			time=0.0;
		}
		public KeySolverTime(String name, double salary) {
			this.time = salary;
			this.solver = name;
		}
		
		@Override
		public void readFields(DataInput dataInput) throws IOException {
			solver = WritableUtils.readString(dataInput);
			time = dataInput.readDouble();
			
		}

		@Override
		public void write(DataOutput out) throws IOException {
			WritableUtils.writeString(out, solver);			
			out.writeDouble(time);			
		}

		@Override
		public int compareTo(KeySolverTime key) {
			int cmp = solver.compareTo(key.solver);
			if(cmp == 0)
			{
				return time.compareTo(key.time);
			
			}
			return cmp;
		}
		
		public String getSolver() {
			return solver;
		}
		public Double getTime() {
			return time;
		}
		
		@Override
		public String toString() {
			return solver+";"+time.toString();
		}
	}	
	//Comparatore per ordiare il nuovo tipo di chiave
	static class ComparatorKeySolverTime extends WritableComparator{
		protected ComparatorKeySolverTime() {
			super(KeySolverTime.class, true);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			KeySolverTime ac = (KeySolverTime) a;
			KeySolverTime bc = (KeySolverTime) b;
			return ac.compareTo(bc);
		}
	}
	
	//Primo Mapper : Legge il file d'input e stampa per ogni tupla il solver,e il tempo impiegato.
	//Le tuple stampate devono avere il valore di RESULT  uguale a 'solved'
	static class Mapper1 extends Mapper<LongWritable, Text, KeySolverTime, Text>{

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, KeySolverTime, Text>.Context context)
				throws IOException, InterruptedException {

			if(key.get() == 0) {
				fileUtil.setHeaders(value.toString());
				return;
			}
			String[] attNames = fileUtil.getSelectedAttributes();
			
			String[] attributes = value.toString().split(fileUtil.getSeparator());

			String newValue="";
			
			for (int i = 0; i < attNames.length;i++) {
				if(i!=fileUtil.getIndexOfAfterSelection("RESULT"))
				
					newValue += attributes[fileUtil.getIndexOf(attNames[i])]+fileUtil.getSeparator();
			}
			
			String result = attributes[fileUtil.getIndexOf("RESULT")];
			KeySolverTime newKey = new KeySolverTime(attributes[fileUtil.getIndexOfKey()],Double.parseDouble(attributes[fileUtil.getIndexOf("Real")]));
			if(result.equals("solved"))
				context.write(newKey,new Text(newValue));
		}
		
	}
	
	//Primo Reducer:
	//Input: Key(Solver;Time), Value
	//Output: Long(Numero occorenza di un solver, Value )
	static class Reducer1 extends Reducer<KeySolverTime,Text, LongWritable, Text>{
		static int cont=0;
		static String lastSolver="";
		@Override
		protected void reduce(KeySolverTime key, Iterable<Text> values, Reducer<KeySolverTime, Text, LongWritable, Text>.Context context)
				throws IOException, InterruptedException {
			
			//se cambia il solver -> reset cont e lastSolver
			if(!lastSolver.equals(key.getSolver()))
			{
				cont=0;
				lastSolver = key.getSolver();
			}
			//conta il numero di istanze di un determinato solver
			for (Text text : values) {
				context.write(new LongWritable(cont), text);
				cont++;
			}
		}
	}
	
	//Secondo Mapper:
	//Stampa (chiave(numero delle istanze) value( solver;time)
	static class Mapper2 extends Mapper<LongWritable, Text, LongWritable, Text>{

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, LongWritable, Text>.Context context)
				throws IOException, InterruptedException {
			
			String[] atts = value.toString().split("\t");
			
			context.write(new LongWritable(Long.parseLong(atts[0])),new Text(atts[1]+";"+atts[2]));

		}
		
	}	
	//Reducer2:
	//Concatena tutti i valori per un certa chiava (numero delle istanze lanciate con il solver)
	static class Reducer2 extends Reducer<LongWritable,Text, LongWritable, Text>{

		@Override
		protected void reduce(LongWritable key, Iterable<Text> values, Reducer<LongWritable, Text, LongWritable, Text>.Context context)
				throws IOException, InterruptedException {
			
			String row="";
			List<String> atts = new ArrayList<>();

			for (Text text : values) {
				
				atts.add(text.toString());
				
			}
			//Ordina i valori
			Collections.sort(atts);
			for (String string : atts) {
				row+=string.split(";")[1]+"\t";
			}
			//Aggiungi i nomi dei solver solo all'inizio
			if(key.get()==0) {
				String header="";
				for (String string : atts) {
					header+=string.split(";")[0]+"\t";
				}
				
				context.write(key, new Text(header));
			}
			context.write(key, new Text(row));
		}
	}
	
	
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "testJob1");

		fileUtil = new JoinFileUtil("Results.txt","\t");


		fileUtil.setSelectedAttributes("Solver\tReal\tRESULT");
		
		fileUtil.setKey("Solver");
//
		job.setSortComparatorClass(ComparatorKeySolverTime.class);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		ChainMapper.addMapper(job, Mapper1.class, LongWritable.class, Text.class,KeySolverTime.class, Text.class, conf);
		ChainReducer.setReducer(job, Reducer1.class, KeySolverTime.class, Text.class, LongWritable.class,Text.class, conf);
		
		if(job.waitForCompletion(true)) {
			job = Job.getInstance(conf,"job2");
			
			job.setMapperClass(Mapper2.class);
			job.setReducerClass(Reducer2.class);
			
			job.setOutputKeyClass(LongWritable.class);
			job.setOutputValueClass(Text.class);
			
			
			FileInputFormat.addInputPath(job, new Path(args[1]));
			FileOutputFormat.setOutputPath(job, new Path(args[2]));
			
			System.exit(job.waitForCompletion(true) ? 0 : 1);
			
		}else
			System.exit(1);
	}
}
