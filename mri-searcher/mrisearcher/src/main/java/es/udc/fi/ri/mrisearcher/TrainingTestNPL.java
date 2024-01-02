package es.udc.fi.ri.mrisearcher;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.es.SpanishAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.search.similarities.LMDirichletSimilarity;
import org.apache.lucene.search.similarities.LMJelinekMercerSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.*;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

public class TrainingTestNPL {
    public TrainingTestNPL() {}

    public static void main(String[] args) throws IOException, ParseException {
        String usage = "java es.udc.fi.ri.mrisearcher.TrainingTestNPL"
                + " -evaljm int1-int2 int3-int4 | -evaldir int1-int2 int3-int4 -cut n -metrica P | R | MRR | MAP -indexin pathname\n\n";
        String evalOption = null;
        String trainingRange = null;
        String testRange = null;
        int cut = -1;
        String metric = null;
        String indexDir = null;
        boolean evaljm = false;
        boolean evaldir = false;

        // Process command line arguments
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-evaljm":
                    evaljm = true;
                    evalOption = "jm";
                    trainingRange = args[++i];
                    testRange = args[++i];
                    break;
                case "-evaldir":
                    evaldir = true;
                    evalOption = "dir";
                    trainingRange = args[++i];
                    testRange = args[++i];
                    break;
                case "-cut":
                    cut = Integer.parseInt(args[++i]);
                    break;
                case "-metrica":
                    metric = args[++i];
                    break;
                case "-indexin":
                    indexDir = args[++i];
                    break;
                default:
                    System.err.println("Invalid argument: " + args[i]);
                    System.exit(1);
            }
        }

        // Validate arguments
        if (evalOption == null) {
            System.err.println("Evaluation option (-evaljm or -evaldir) is missing");
            System.err.println("Usage: " + usage);
            System.exit(1);
        }
        if (trainingRange == null || testRange == null) {
            System.err.println("Invalid range");
            System.err.println("Usage: " + usage);
            System.exit(1);
        }
        if (evaljm && evaldir) {
            System.err.println("Choose only one evaluation option (-evaljm or -evaldir)");
            System.err.println("Usage: " + usage);
            System.exit(1);
        }
        if (cut < 0) {
            System.err.println("Cut value (-cut) is missing or invalid");
            System.err.println("Usage: " + usage);
            System.exit(1);
        }
        if (metric == null || (!metric.equals("P") && !metric.equals("R") && !metric.equals("MRR") && !metric.equals("MAP"))) {
            System.err.println("Metric (-metrica) is missing/invalid");
            System.err.println("Usage: " + usage);
            System.exit(1);
        }
        if (indexDir == null) {
            System.err.println("Index directory (-indexin) is missing");
            System.err.println("Usage: " + usage);
            System.exit(1);
        }

        // Execute selected evaluation method
        Similarity luceneSimilarity = null;

        DirectoryReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexDir)));

        IndexSearcher searcher = new IndexSearcher(reader);

        String analyzerType = null;
        String stopwordsPath = null;
        try (BufferedReader br = new BufferedReader(new FileReader(indexDir+"-analyzer"))) {
            analyzerType = br.readLine();
            if (analyzerType != null) {
                analyzerType = analyzerType.trim(); // Eliminar espacios al inicio y al final de la línea
            }
            if (analyzerType.equals("stop")){
                stopwordsPath = br.readLine().trim();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        Analyzer analyzer = null;

        switch (analyzerType) {
            case "standard":
                analyzer = new StandardAnalyzer();
                break;
            case "simple":
                analyzer = new SimpleAnalyzer();
                break;
            case "stop":
                if (stopwordsPath != null) {
                    analyzer = new StopAnalyzer(Paths.get(stopwordsPath));
                } else {
                    analyzer = new StopAnalyzer(CharArraySet.EMPTY_SET);
                }
                break;
            case "whitespace":
                analyzer = new WhitespaceAnalyzer();
                break;
            case "keyword":
                analyzer = new KeywordAnalyzer();
                break;
            case "english":
                analyzer = new EnglishAnalyzer();
                break;
            case "spanish":
                analyzer = new SpanishAnalyzer();
                break;
            default:
                throw new IllegalArgumentException("Unknown analyzer: " + analyzer);
        }

        QueryParser parser = new QueryParser("Contents", analyzer);

        List<String> trainingQueries;
        List<String> testQueries;
        trainingQueries = readQueriesFromFile("query-text", trainingRange);
        testQueries = readQueriesFromFile("query-text", testRange);

        String[] trainingRangeArray = trainingRange.split("-");
        String[] testRangeArray = testRange.split("-");

        int queryStartTraining = Integer.parseInt(trainingRangeArray[0]);
        int queryStartTest = Integer.parseInt(testRangeArray[0]);

        ArrayList<ArrayList<String>> relevantesPorQueryTraining = readRelevanceJudgmentsFromFile("rlv-ass", trainingRange);
        ArrayList<ArrayList<String>> relevantesPorQueryTest = readRelevanceJudgmentsFromFile("rlv-ass", testRange);

        List<String> documents = new ArrayList<>();
        List<String> relevantDocs = new ArrayList<>();
        if (evalOption.equals("jm")) {
            float[] array = {0.0f, 0.1f, 0.2f, 0.3f, 0.4f, 0.5f, 0.6f, 0.7f, 0.8f, 0.9f, 1.0f};
            trainAndTest(array, luceneSimilarity, searcher, trainingQueries, testQueries, documents, relevantDocs,
                                        relevantesPorQueryTraining, relevantesPorQueryTest, parser, metric, cut, evalOption, trainingRange, testRange, queryStartTraining, queryStartTest);
        } else if (evalOption.equals("dir")) {
            // Execute Dirichlet evaluation
            float[] array = {0, 200, 400, 600, 800, 1000, 1500, 2000, 2500, 3000, 4000};
            trainAndTest(array, luceneSimilarity, searcher, trainingQueries, testQueries, documents, relevantDocs,
                                        relevantesPorQueryTraining, relevantesPorQueryTest, parser, metric, cut, evalOption, trainingRange, testRange, queryStartTraining, queryStartTest);

        }
    }
    private static void trainAndTest(float[] array, Similarity luceneSimilarity, IndexSearcher searcher, List<String> trainingQueries,
                                      List<String> testQueries, List<String> documents, List<String> relevantDocs,
                                      ArrayList<ArrayList<String>> relevantesPorQueryTraining,
                                      ArrayList<ArrayList<String>> relevantesPorQueryTest, QueryParser parser, String metric, int cut,
                                      String evalOption, String trainingRange, String testRange, int queryStartTraining, int queryStartTest) throws ParseException, IOException {
        String line;
        float[] metricsArray = new float[array.length];
        List<List<Float>> resultadosPorValor = new ArrayList<>();
        PrintWriter writer2 = new PrintWriter(System.out);
        for (int i = 0; i < array.length; i++) {
            if(evalOption.equals("dir")){
                luceneSimilarity = new LMDirichletSimilarity((int)array[i]);
            } else if (evalOption.equals("jm") && array[i]!=0){
                luceneSimilarity = new LMJelinekMercerSimilarity(array[i]);
            }
            if(luceneSimilarity!=null) {
                searcher.setSimilarity(luceneSimilarity);
            }
            float metr;
            List<Float> metricsList = new ArrayList<>();
            int cont = 0;
            while (cont < trainingQueries.size()) {
                documents.clear();
                line = trainingQueries.get(cont);
                relevantDocs = relevantesPorQueryTraining.get(cont);
                cont++;

                if (line == null) {
                    break;
                }

                line = line.trim();
                if (line.length() == 0) {
                    break;
                }

                // Parsear la query
                Query query = parser.parse(line);

                // Realizar la búsqueda
                TotalHitCountCollector totalHitCountCollector = new TotalHitCountCollector();
                searcher.search(query, totalHitCountCollector);
                int totalHits = totalHitCountCollector.getTotalHits();
                TopDocs topDocs = searcher.search(query, cut);
                ScoreDoc[] hits = topDocs.scoreDocs;

                for (int j = 0; j < hits.length && j < cut; ++j) {
                    int docId = hits[j].doc;
                    Document d = searcher.doc(docId);
                    documents.add(d.get("DocIDNPL"));
                }

                switch(metric) {
                    case "P":
                        metr = getPrecision(cut, relevantDocs, documents);
                        metricsList.add(metr);
                        break;
                    case "R":
                        metr = getRecall(cut, relevantDocs, documents);
                        metricsList.add(metr);
                        break;
                    case "MRR":
                        metr = getReciprocalRank(cut, relevantDocs, documents);
                        metricsList.add(metr);
                        break;
                    case "MAP":
                        metr = getAveragePrecision(cut, relevantDocs, documents);
                        metricsList.add(metr);
                        break;
                    default:
                        System.out.println("Opción inválida");
                        break;
                }

            }
            resultadosPorValor.add(metricsList);
            metricsArray[i] = getMean(metricsList);
        }
        float max = metricsArray[0]; // inicializar max con el primer elemento del array
        int maxi = 0;
        for (int i = 1; i < array.length; i++) { // iterar por el array desde el segundo elemento
            if (metricsArray[i] > max) { // si el elemento actual es mayor que el máximo, actualizar max
                max = metricsArray[i];
                maxi = i;
            }
        }
        float valorMax = array[maxi];

        if(evalOption.equals("dir")){
            luceneSimilarity = new LMDirichletSimilarity((int)valorMax);
        } else if (evalOption.equals("jm")){
            luceneSimilarity = new LMJelinekMercerSimilarity(valorMax);
        }
        searcher.setSimilarity(luceneSimilarity);

        List<Float> metricsList2 = new ArrayList<>();
        float metr;
        int cont = 0;
        while (cont < testQueries.size()) {
            documents.clear();
            line = testQueries.get(cont);
            relevantDocs = relevantesPorQueryTest.get(cont);
            cont++;

            if (line == null) {
                break;
            }

            line = line.trim();
            if (line.length() == 0) {
                break;
            }

            // Parsear la query
            Query query = parser.parse(line);

            // Realizar la búsqueda
            TotalHitCountCollector totalHitCountCollector = new TotalHitCountCollector();
            searcher.search(query, totalHitCountCollector);
            int totalHits = totalHitCountCollector.getTotalHits();
            int top = 100;
            TopDocs topDocs = searcher.search(query, top);
            ScoreDoc[] hits = topDocs.scoreDocs;

            for (int j = 0; j < hits.length && j < top; ++j) {
                int docId = hits[j].doc;
                Document d = searcher.doc(docId);
                documents.add(d.get("DocIDNPL"));
            }

            switch(metric) {
                case "P":
                    metr = getPrecision(cut, relevantDocs, documents);
                    metricsList2.add(metr);
                    break;
                case "R":
                    metr = getRecall(cut, relevantDocs, documents);
                    metricsList2.add(metr);
                    break;
                case "MRR":
                    metr = getReciprocalRank(cut, relevantDocs, documents);
                    metricsList2.add(metr);
                    break;
                case "MAP":
                    metr = getAveragePrecision(cut, relevantDocs, documents);
                    metricsList2.add(metr);
                    break;
                default:
                    System.out.println("Opción inválida");
                    break;
            }

        }
        float metricsMean = getMean(metricsList2);
        File csvFile = null;
        if(metric.equals("MRR")) {
            csvFile = new File("npl." + evalOption + ".training." + trainingRange + ".test." + testRange + "." + metric.toLowerCase(Locale.ROOT) + ".training.csv");
        } else {
            csvFile = new File("npl." + evalOption + ".training." + trainingRange + ".test." + testRange + "." + metric.toLowerCase(Locale.ROOT) + cut + ".training.csv");
        }
        try (FileWriter writer = new FileWriter(csvFile)) {
            // Escribir cabecera
            if(metric.equals("MRR")) {
                writer.append(metric + ",");
            } else {
                writer.append(metric + "@" + cut + ",");
            }

            if(evalOption.equals("dir")){
                for (int i = 0; i < array.length; i++) {
                    writer.append("mu_"+ (int)array[i]);
                    if (i != array.length - 1) {
                        writer.append(",");
                    }
                }
            } else if (evalOption.equals("jm")){
                for (int i = 0; i < array.length; i++) {
                    writer.append("lambda_"+ array[i]);
                    if (i != array.length - 1) {
                        writer.append(",");
                    }
                }
            }
            writer.append("\n");

            // Escribir resultados por query
            for (int i = 0; i < trainingQueries.size(); i++) {
                writer.append(queryStartTraining + ",");
                queryStartTraining++;
                for (int j = 0; j < resultadosPorValor.size(); j++) {
                    List<Float> queryResults = resultadosPorValor.get(j);
                    writer.append(String.valueOf(queryResults.get(i)));
                    if (j != resultadosPorValor.size() - 1) {
                        writer.append(",");
                    }
                }
                writer.append("\n");
            }

            // Escribir fila de promedios
            writer.append("Promedio,");
            for (int i = 0; i < metricsArray.length; i++) {
                writer.append(String.valueOf(metricsArray[i]));
                if (i != resultadosPorValor.size() - 1) {
                    writer.append(",");
                }
            }
            writer.append("\n");

            writer.flush();
            System.out.println("Archivo .csv generado correctamente.");
            // Crear un BufferedReader para leer el archivo CSV
            BufferedReader reader = new BufferedReader(new FileReader(csvFile));

            // Leer cada línea del archivo CSV y escribirla en la salida estándar
            String linea;
            while ((linea = reader.readLine()) != null) {
                writer2.println(linea);
            }

            // Cerrar el BufferedReader y el PrintWriter
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }


        File csvFile2 = null;
        if(metric.equals("MRR")) {
            csvFile2 = new File("npl." + evalOption + ".training." + trainingRange + ".test." + testRange + "." + metric.toLowerCase(Locale.ROOT) + ".test.csv");
        } else {
            csvFile2 = new File("npl." + evalOption + ".training." + trainingRange + ".test." + testRange + "." + metric.toLowerCase(Locale.ROOT) + cut + ".test.csv");
        }
        try (FileWriter writer = new FileWriter(csvFile2)) {
            if(evalOption.equals("dir")){
                writer.append("mu_" + (int)valorMax + ",");
            } else if (evalOption.equals("jm")){
                writer.append("jm_" + valorMax +  ",");
            }

            if(metric.equals("MRR")) {
                writer.append(metric + "\n");
            } else {
                writer.append(metric + "@" + cut + "\n");
            }

            // Escribir resultados por query
            for (int i = 0; i < testQueries.size(); i++) {
                writer.append(queryStartTest + "," + metricsList2.get(i) + "\n");
                queryStartTest++;
            }

            // Escribir fila de promedios
            writer.append("Promedio," + metricsMean + "\n");

            writer.flush();
            System.out.println("Archivo .csv generado correctamente.");
            // Crear un BufferedReader para leer el archivo CSV
            BufferedReader reader = new BufferedReader(new FileReader(csvFile2));

            // Leer cada línea del archivo CSV y escribirla en la salida estándar
            String linea;
            while ((linea = reader.readLine()) != null) {
                writer2.println(linea);
            }

            // Cerrar el BufferedReader y el PrintWriter
            reader.close();
            writer2.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    private static float getMean(List<Float> metrics){
        float resultado;
        int cont = 0;
        float suma = 0f;
        for (float p: metrics) {
            if (p != 0f) {
                cont++;
                suma = suma + p;
            }
        }
        if (cont == 0) {
            resultado = 0f;
        } else {
            resultado = suma / (float) cont;
        }
        return resultado;
    }
    private static float getPrecision(int cut, List<String> relevantDocs, List<String> documents) {
        // inicializa un contador para el número de documentos en ambas listas
        int relevantes = 0;
        float resultado;

        // recorre todos los documentos y comprueba si están en la lista de documentos relevantes
        int cont = 1;
        for (String doc : documents) {
            if(cont > cut){
                break;
            }
            if (relevantDocs.contains(doc)) {
                relevantes++;
            }
            cont++;
        }
        resultado = (float)relevantes / (float)cut;

        return resultado;
    }

    private static float getRecall(int cut, List<String> relevantDocs, List<String> documents) {
        // inicializa un contador para el número de documentos en ambas listas
        int relevantes = 0;
        float resultado;

        // recorre todos los documentos y comprueba si están en la lista de documentos relevantes
        int cont = 1;
        for (String doc : documents) {
            if(cont > cut){
                break;
            }
            if (relevantDocs.contains(doc)) {
                relevantes++;
            }
            cont++;
        }
        if(relevantDocs.size() == 0){
            resultado = 0f;
        }else {
            resultado = (float) relevantes / (float) relevantDocs.size();
        }
        return resultado;
    }

    private static float getReciprocalRank(int cut, List<String> relevantDocs, List<String> documents) {
        float resultado = 0f;

        // recorre todos los documentos y comprueba si están en la lista de documentos relevantes
        int cont = 1;
        for (String doc : documents) {
            if(cont > cut){
                break;
            }
            if (relevantDocs.contains(doc)) {
                resultado = 1/(float)cont;
                break;
            }
            cont++;
        }

        return resultado;
    }

    private static float getAveragePrecision(int cut, List<String> relevantDocs, List<String> documents) {
        // inicializa un contador para el número de documentos en ambas listas
        int relevantes = 0;
        float resultado;
        float numerador = 0f;

        // recorre todos los documentos y comprueba si están en la lista de documentos relevantes
        int cont = 1;
        for (String doc : documents) {
            if(cont > cut){
                break;
            }
            if (relevantDocs.contains(doc)) {
                relevantes++;
                numerador += (float)relevantes / (float)cont;
            }
            cont++;
        }
        resultado =  numerador / (float)relevantDocs.size();

        return resultado;
    }

    private static List<String> readQueriesFromFile(String queryFile, String queryOption) throws IOException {//REVISAR SALTOS DE LINEA
        List<String> queries = new ArrayList<>();
        BufferedReader reader = new BufferedReader(new FileReader(queryFile));
        String line;
        int lineNum = 1;
        int queryStart = -1;
        int queryEnd = -1;

        // Parsear la opción -queries para determinar el rango de queries a leer
        if (queryOption.matches("\\d+")) {
            // Si se proporciona un número entero, leer solo esa query
            queryStart = Integer.parseInt(queryOption);
            queryEnd = Integer.parseInt(queryOption);
        } else if (queryOption.matches("\\d+-\\d+")) {
            // Si se proporciona un rango de números, leer el rango de queries
            String[] range = queryOption.split("-");
            queryStart = Integer.parseInt(range[0]);
            queryEnd = Integer.parseInt(range[1]);
        }

        // Leer las queries del archivo
        while ((reader.readLine()) != null) {
            line = reader.readLine();
            // Leer solo las queries correspondientes al rango indicado
            if (queryOption.equals("all") || (lineNum >= queryStart && lineNum <= queryEnd)) {
                queries.add(line.toLowerCase(Locale.ROOT));
            }
            reader.readLine();
            lineNum++;
        }
        reader.close();
        return queries;
    }

    private static ArrayList<ArrayList<String>> readRelevanceJudgmentsFromFile(String rlvAssFile, String queryOption) throws IOException {//REVISAR SALTOS DE LINEA
        ArrayList<ArrayList<String>> relevantesPorQuery = new ArrayList<>();
        BufferedReader reader = new BufferedReader(new FileReader(rlvAssFile));
        String line;
        int lineNum = 1;
        int queryStart = -1;
        int queryEnd = -1;

        // Parsear la opción -queries para determinar el rango de queries a leer
        if (queryOption.matches("\\d+")) {
            // Si se proporciona un número entero, leer solo esa query
            queryStart = Integer.parseInt(queryOption);
            queryEnd = Integer.parseInt(queryOption);
        } else if (queryOption.matches("\\d+-\\d+")) {
            // Si se proporciona un rango de números, leer el rango de queries
            String[] range = queryOption.split("-");
            queryStart = Integer.parseInt(range[0]);
            queryEnd = Integer.parseInt(range[1]);
        }

        String[] numeros;
        // Leer las queries del archivo
        while ((reader.readLine()) != null) {
            // Leer solo las queries correspondientes al rango indicado
            line = reader.readLine();
            ArrayList<String> numerosList = new ArrayList<>();
            if (queryOption.equals("all") || (lineNum >= queryStart && lineNum <= queryEnd)) {
                while(!(line.trim().equals("/"))){
                    numeros = line.trim().split("\\s+");
                    numerosList.addAll(Arrays.asList(numeros));
                    line = reader.readLine();
                }
                relevantesPorQuery.add(new ArrayList<>(numerosList));
                numerosList.clear();
            }
            while(!(line.trim().equals("/"))){
                line = reader.readLine();
            }
            lineNum++;
        }
        reader.close();
        return relevantesPorQuery;
    }

}


