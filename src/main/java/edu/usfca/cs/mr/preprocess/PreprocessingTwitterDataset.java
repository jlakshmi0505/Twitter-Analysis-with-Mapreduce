package edu.usfca.cs.mr.preprocess;
import java.io.*;


public class PreprocessingTwitterDataset {
    public static void main(String[] args) throws IOException {
        File file = new File(args[0]);
        if (args.length > 1) {
            BufferedReader br = new BufferedReader(new FileReader(file));
            String st;
            StringBuilder sb = new StringBuilder();
            StringBuilder stFinal = new StringBuilder();
            while ((st = br.readLine()) != null) {
                if (st.equals("")) {
                    stFinal.append(sb);
                    stFinal.append("\n");
                    sb = new StringBuilder();
                } else {
                    sb.append(st);
                    sb.append(",");
                }
            }
            writeToFile(args[1],stFinal);
        }
    }
    private static void writeToFile(String fileName,StringBuilder stringBuilder) throws IOException {
        FileWriter myWriter = new FileWriter(fileName);
        myWriter.write(stringBuilder.toString());
        myWriter.close();
        System.out.println("Successfully wrote to the file.");
    }
}
