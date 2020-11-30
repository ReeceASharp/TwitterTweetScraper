import java.io.*;

public class SplitTest {
    public static void main(String[] args) throws IOException {
        String path = "input/test.csv";
        File file = new File(path);

        BufferedReader br = new BufferedReader(new FileReader(file));

        String line;
        while ((line = br.readLine()) != null) {
            String[] res = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);

            if (res.length != 36) {
                continue;
                //System.out.printf("Error (%s) input: %s%n", res.length, line);
            }

            if (!res[11].equals("en")) {
                System.out.println("Not English: '" + res[11] + "' | " + line);
                continue;
            }

            System.out.printf("Current Split Length: %d%n", res.length);


//            String id = res[0];
//            String creationDate = res[2];
//            String tweet = res[10];
//
//            String cleanedData = String.format("%s,%s,%s", id, creationDate, tweet);
//            System.out.println(cleanedData);

        }
    }

}
