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
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.search.similarities.LMDirichletSimilarity;
import org.apache.lucene.search.similarities.LMJelinekMercerSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.FSDirectory;

import java.io.*;
import java.nio.file.Paths;
import java.util.*;

public class SearchEvalNPL {
    private SearchEvalNPL() {}

    public static void main(String[] args) throws Exception {
        String usage = "java es.udc.fi.ri.mrisearcher.SearchEvalNPL"
                + " -indexin INDEX_PATH -search jm LAMBDA_VALUE | dir MU_VALUE -cut N -top M [-queries all | int1 | int1-int2]\n\n";
        String indexPath = null;
        boolean search = false;
        float jmLambda = -1;
        float dirMu = -1;
        int cut = -1;
        int top = -1;
        String queryOption = "all";
        String modelType = "";

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-indexin":
                    indexPath = args[++i];
                    break;
                case "-search":
                    search = true;
                    modelType = args[++i];
                    if (modelType.equals("jm")) {
                        jmLambda = Float.parseFloat(args[++i]);
                    } else if (modelType.equals("dir")) {
                        dirMu = Float.parseFloat(args[++i]);
                    } else {
                        throw new IllegalArgumentException("Unknown model type: " + modelType);
                    }
                    break;
                case "-cut":
                    cut = Integer.parseInt(args[++i]);
                    break;
                case "-top":
                    top = Integer.parseInt(args[++i]);
                    break;
                case "-queries":
                    queryOption = args[++i];
                    break;
                default:
                    throw new IllegalArgumentException("Unknown parameter: " + args[i]);
            }
        }

        if (top == -1 || cut == -1 || indexPath == null || !search) {
            System.err.println("Usage: " + usage);
            System.exit(1);
        }

        Similarity luceneSimilarity = null;
        if (jmLambda != -1 && jmLambda != 0) {
            luceneSimilarity = new LMJelinekMercerSimilarity(jmLambda);
        } else if (dirMu != -1) {
            luceneSimilarity = new LMDirichletSimilarity(dirMu);
        }

        DirectoryReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexPath)));
        IndexSearcher searcher = new IndexSearcher(reader);
        if(luceneSimilarity!=null){
            searcher.setSimilarity(luceneSimilarity);
        }
        String analyzerType = null;
        String stopwordsPath = null;
        try (BufferedReader br = new BufferedReader(new FileReader(indexPath+"-analyzer"))) {
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
        String line;
        List<String> queries;
        queries = readQueriesFromFile("query-text", queryOption);
        File outFile = null;
        File csvFile = null;

        if (modelType.equals("jm")) {
            outFile = new File("npl.jm." + top + ".hits.lambda." + jmLambda + ".q" + queryOption + ".txt");
        } else if (modelType.equals("dir")) {
            outFile = new File("npl.dir." + top + ".hits.mu." + (int)dirMu + ".q" + queryOption + ".txt");
        }
        List<String> documents = new ArrayList<>();
        ArrayList<ArrayList<String>> relevantesPorQuery = readRelevanceJudgmentsFromFile("rlv-ass", queryOption);
        List<String> relevantDocs;

        if (modelType.equals("jm")) {
            csvFile = new File("npl.jm." + cut + ".cut.lambda." + jmLambda + ".q" + queryOption + ".csv");
        } else if (modelType.equals("dir")) {
            csvFile = new File("npl.dir." + cut + ".cut.mu." + (int)dirMu + ".q" + queryOption + ".csv");
        }

        FileWriter csvWriter = new FileWriter(csvFile);

        // Escribir la primera fila del archivo CSV
        csvWriter.append("Query,P@10,Recall@10,RR,AP@10\n");

        List<Float> precisionsList = new ArrayList<>();
        List<Float> recallsList = new ArrayList<>();
        List<Float> rrList = new ArrayList<>();
        List<Float> apList = new ArrayList<>();
        float precision;
        float recall;
        float rr;
        float ap;
        int cont = 0;
        int queryStart = 1;
        if (queryOption.matches("\\d+")) {
            // Si se proporciona un número entero, leer solo esa query
            queryStart = Integer.parseInt(queryOption);
        } else if (queryOption.matches("\\d+-\\d+")) {
            // Si se proporciona un rango de números, leer el rango de queries
            String[] range = queryOption.split("-");
            queryStart = Integer.parseInt(range[0]);
        }

        while (cont < queries.size()) {
            documents.clear();
            line = queries.get(cont);
            relevantDocs = relevantesPorQuery.get(cont);
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

            System.out.println(queryStart + ". Searching for: " + query.toString("Contents"));
            // Realizar la búsqueda

            TotalHitCountCollector totalHitCountCollector = new TotalHitCountCollector();
            searcher.search(query, totalHitCountCollector);
            int totalHits = totalHitCountCollector.getTotalHits();
            System.out.println("Total Results : " + totalHits);

            TopDocs topDocs = searcher.search(query, top);
            ScoreDoc[] hits = topDocs.scoreDocs;

            PrintWriter writer = new PrintWriter(new FileWriter(outFile, true)); // true para indicar que se agregan al final

            writer.println("Results for: " + query.toString("Contents"));
            for (int i = 0; i < hits.length && i < top; ++i) {
                int docId = hits[i].doc;
                Document d = searcher.doc(docId);
                String docLine;
                if(relevantDocs.contains(d.get("DocIDNPL"))){
                    docLine = (i + 1) + ". DocIDNPL: " + d.get("DocIDNPL") + ". Contents: " + d.get("Contents")+ ". Score=" + hits[i].score + ". RELEVANTE";
                }else{
                    docLine = (i + 1) + ". DocIDNPL: " + d.get("DocIDNPL") + ". Contents: " + d.get("Contents")+ ". Score=" + hits[i].score + ".";
                }
                documents.add(d.get("DocIDNPL"));
                writer.println(docLine);
                System.out.println(docLine);
            }
            writer.println("");

            // Cerrar el escritor de archivos
            writer.close();

            precision = getPrecision(cut, relevantDocs, documents);
            precisionsList.add(precision);
            recall = getRecall(cut, relevantDocs, documents);
            recallsList.add(recall);
            rr = getReciprocalRank(cut, relevantDocs, documents);
            rrList.add(rr);
            ap = getAveragePrecision(cut, relevantDocs, documents);
            apList.add(ap);

            System.out.println("P@" + cut + ": " + precision);
            System.out.println("Recall@" + cut + ": " + recall);
            System.out.println("RR: " + rr);
            System.out.println("AP@" + cut + ": " + ap);
            System.out.println("----------------------------------------------------");

            csvWriter.append(String.valueOf(queryStart));
            csvWriter.append(",");
            csvWriter.append(String.valueOf(precision));
            csvWriter.append(",");
            csvWriter.append(String.valueOf(recall));
            csvWriter.append(",");
            csvWriter.append(String.valueOf(rr));
            csvWriter.append(",");
            csvWriter.append(String.valueOf(ap));
            csvWriter.append("\n");

            queryStart++;

        }
        System.out.println("Métricas promediadas:");
        float mprecision = getMean(precisionsList);
        float mrecall = getMean(recallsList);
        float mrr = getMean(rrList);
        float map = getMean(apList);

        System.out.println("Mean P@" + cut + ": " + mprecision);
        System.out.println("Mean Recall@" + cut + ": " + mrecall);
        System.out.println("MRR: " + mrr);
        System.out.println("MAP@" + cut + ": " + map);
        System.out.println("----------------------------------------------------");

        // Escribir la fila de promedios al final del archivo CSV
        csvWriter.append("Promedio,");
        csvWriter.append(String.valueOf(mprecision));
        csvWriter.append(",");
        csvWriter.append(String.valueOf(mrecall));
        csvWriter.append(",");
        csvWriter.append(String.valueOf(mrr));
        csvWriter.append(",");
        csvWriter.append(String.valueOf(map));

        csvWriter.flush();
        csvWriter.close();
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
