
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package es.udc.fi.ri.mrisearcher;


import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.es.SpanishAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.demo.knn.DemoEmbeddings;
import org.apache.lucene.demo.knn.KnnVectorDict;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnVectorField;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.search.similarities.LMDirichletSimilarity;
import org.apache.lucene.search.similarities.LMJelinekMercerSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.IOUtils;

/**
 * Index all text files under a directory.
 *
 * <p>This is a command-line application demonstrating simple Lucene indexing. Run it with no
 * command-line arguments for usage information.
 */
public class DenseRetrieval implements AutoCloseable {
    static final String KNN_DICT = "knn-dict";

    // Calculates embedding vectors for KnnVector search
    private final DemoEmbeddings demoEmbeddings;
    private final KnnVectorDict vectorDict;

    private DenseRetrieval(KnnVectorDict vectorDict) throws IOException {
        if (vectorDict != null) {
            this.vectorDict = vectorDict;
            demoEmbeddings = new DemoEmbeddings(vectorDict);
        } else {
            this.vectorDict = null;
            demoEmbeddings = null;
        }
    }

    /** Index all text files under a directory. */
    public static void main(String[] args) throws Exception {
        String usage = "java es.udc.fi.ri.mrisearcher.DenseRetrieval"
                + " [-openmode OPEN_MODE] [-index INDEX_PATH] -docs DOCS_PATH [-analyzer ANALYZER] [-stopwords STOPWORDS_PATH] -knn_dict KNNDICT -knn_vector knnHits -cut N -top M -indexingmodel jm LAMBDA_VALUE | dir MU_VALUE [-queries all | int1 | int1-int2]\n\n"
                + "This indexes the documents in DOCS_PATH using the specified analyzer and similarity model,"
                + "creating a Lucene index in INDEX_PATH that can be searched with the specified model.\n"
                + "Supported analyzers: standard, simple, whitespace, keyword, english\n"
                + "Supported similarity models: jm lambda, dir mu";
        String indexPath = "index2";
        String docsPath = null;
        String openmode = "create_or_append";
        String analyzer = "standard";
        String stopwordsPath = null;
        boolean indexingmodel = false;
        float jmLambda = -1;
        float dirMu = -1;
        String vectorDictSource = null;
        int cut = -1;
        int top = -1;
        String queryOption = "all";
        String modelType = "";
        int knnVectors = 0;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-openmode":
                    openmode = args[++i];
                    break;
                case "-index":
                    indexPath = args[++i];
                    break;
                case "-docs":
                    docsPath = args[++i];
                    break;
                case "-analyzer":
                    analyzer = args[++i];
                    break;
                case "-stopwords":
                    stopwordsPath = args[++i];
                    break;
                case "-indexingmodel":
                    indexingmodel = true;
                    modelType = args[++i];
                    if (modelType.equals("jm")) {
                        jmLambda = Float.parseFloat(args[++i]);
                    } else if (modelType.equals("dir")) {
                        dirMu = Float.parseFloat(args[++i]);
                    } else {
                        throw new IllegalArgumentException("Unknown model type: " + modelType);
                    }
                    break;
                case "-knn_dict":
                    vectorDictSource = args[++i];
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
                case "-knn_vector":
                    knnVectors = Integer.parseInt(args[++i]);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown parameter: " + args[i]);
            }
        }

        if (docsPath == null || top == -1 || cut == -1 || !indexingmodel || knnVectors == 0 | vectorDictSource == null) {
            System.out.println("Usage: " + usage);
            System.exit(1);
        }

        Analyzer luceneAnalyzer = null;
        switch (analyzer) {
            case "standard":
                luceneAnalyzer = new StandardAnalyzer();
                break;
            case "simple":
                luceneAnalyzer = new SimpleAnalyzer();
                break;
            case "stop":
                if (stopwordsPath != null) {
                    luceneAnalyzer = new StopAnalyzer(Paths.get(stopwordsPath));
                } else {
                    luceneAnalyzer = new StopAnalyzer(CharArraySet.EMPTY_SET);
                }
                break;
            case "whitespace":
                luceneAnalyzer = new WhitespaceAnalyzer();
                break;
            case "keyword":
                luceneAnalyzer = new KeywordAnalyzer();
                break;
            case "english":
                luceneAnalyzer = new EnglishAnalyzer();
                break;
            case "spanish":
                luceneAnalyzer = new SpanishAnalyzer();
                break;
            default:
                throw new IllegalArgumentException("Unknown analyzer: " + analyzer);
        }
        // Set up the similarity based on the input argument
        Similarity luceneSimilarity = null;
        if (jmLambda != -1 && jmLambda != 0) {
            luceneSimilarity = new LMJelinekMercerSimilarity(jmLambda);
        } else if (dirMu != -1) {
            luceneSimilarity = new LMDirichletSimilarity(dirMu);
        }

// Create or append to the index based on the open mode
        IndexWriterConfig iwc = new IndexWriterConfig(luceneAnalyzer);
        switch (openmode) {
            case "create":
                iwc.setOpenMode(OpenMode.CREATE);
                break;
            case "append":
                iwc.setOpenMode(OpenMode.APPEND);
                break;
            case "create_or_append":
                iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
                break;
            default:
                throw new IllegalArgumentException("Unknown open mode: " + openmode);
        }

        //Convierte la ruta de String a Path
        final Path docDir = Paths.get(docsPath);
        if (!Files.isReadable(docDir)) {
            System.out.println(
                    "Document directory '"
                            + docDir.toAbsolutePath()
                            + "' does not exist or is not readable, please check the path");
            System.exit(1);
        }

        Date start = new Date();
        try {
            System.out.println("Indexing to directory '" + indexPath + "'...");

            Directory dir = FSDirectory.open(Paths.get(indexPath));

            if(luceneSimilarity!=null){
                iwc.setSimilarity(luceneSimilarity);
            }
            // Optional: for better indexing performance, if you
            // are indexing many documents, increase the RAM
            // buffer.  But if you do this, increase the max heap
            // size to the JVM (eg add -Xmx512m or -Xmx1g):
            //
            // iwc.setRAMBufferSizeMB(256.0);

            KnnVectorDict vectorDictInstance = null;
            long vectorDictSize = 0;
            KnnVectorDict.build(Paths.get(vectorDictSource), dir, KNN_DICT);
            vectorDictInstance = new KnnVectorDict(dir, KNN_DICT);
            vectorDictSize = vectorDictInstance.ramBytesUsed();

            try (IndexWriter writer = new IndexWriter(dir, iwc);
                 DenseRetrieval indexFiles = new DenseRetrieval(vectorDictInstance)) {
                indexFiles.indexDocs(writer, docDir);

                // NOTE: if you want to maximize search performance,
                // you can optionally call forceMerge here.  This can be
                // a terribly costly operation, so generally it's only
                // worth it when your index is relatively static (ie
                // you're done adding documents to it):
                //
                // writer.forceMerge(1);
            } finally {
                IOUtils.close(vectorDictInstance);
            }

            Date end = new Date();
            try (IndexReader reader = DirectoryReader.open(dir)) {
                System.out.println(
                        "Indexed "
                                + reader.numDocs()
                                + " documents in "
                                + (end.getTime() - start.getTime())
                                + " milliseconds");
                if (reader.numDocs() > 100
                        && vectorDictSize < 1_000_000
                        && System.getProperty("smoketester") == null) {
                    throw new RuntimeException(
                            "Are you (ab)using the toy vector dictionary? See the package javadocs to understand why you got this exception.");
                }
            }

            DirectoryReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexPath)));
            IndexSearcher searcher = new IndexSearcher(reader);
            KnnVectorDict vectorDict = null;
            vectorDict = new KnnVectorDict(reader.directory(), DenseRetrieval.KNN_DICT);
            searcher.setSimilarity(luceneSimilarity);

            QueryParser parser = new QueryParser("Contents", luceneAnalyzer);
            String line;
            List<String> queries;
            queries = readQueriesFromFile("query-text", queryOption);
            File outFile = null;
            File csvFile = null;

            if (modelType.equals("jm")) {
                outFile = new File("npl.jm." + top + ".hits.lambda." + jmLambda + ".q" + queryOption + "."+ vectorDictSource + ".knn." + knnVectors + ".txt");
            } else if (modelType.equals("dir")) {
                outFile = new File("npl.dir." + top + ".hits.mu." + (int)dirMu + ".q" + queryOption + "."+ vectorDictSource + ".knn." + knnVectors + ".txt");
            }
            List<String> documents = new ArrayList<>();
            ArrayList<ArrayList<String>> relevantesPorQuery = readRelevanceJudgmentsFromFile("rlv-ass", queryOption);
            List<String> relevantDocs;

            if (modelType.equals("jm")) {
                csvFile = new File("npl.jm." + cut + ".cut.lambda." + jmLambda + ".q" + queryOption + "."+ vectorDictSource + ".knn." + knnVectors + ".csv");
            } else if (modelType.equals("dir")) {
                csvFile = new File("npl.dir." + cut + ".cut.mu." + (int)dirMu + ".q" + queryOption + "."+ vectorDictSource + ".knn." + knnVectors + ".csv");
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
                query = addSemanticQuery(query, vectorDict, knnVectors);

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

        } catch (IOException e) {
            System.out.println(" caught a " + e.getClass() + "\n with message: " + e.getMessage());
        }
    }

    void indexDocs(final IndexWriter writer, Path path) throws IOException {
        if (Files.isRegularFile(path)) {
            // Parse el archivo que contiene los documentos NPL
            parseNPLFile(path, writer);
        }
    }

    // Parsea el archivo que contiene los documentos NPL y retorna una lista de documentos
    void parseNPLFile(Path file, final IndexWriter writer) throws IOException {
        try (BufferedReader br = Files.newBufferedReader(file)) {
            String line;
            String docIDNPL = null;
            StringBuilder contents = new StringBuilder();

            while ((line = br.readLine()) != null) {
                if (docIDNPL == null) {
                    // Extract docId from the first line of each document
                    docIDNPL = line;
                } else if (line.trim().equals("/")) {
                    // Extract content until encountering a line with only "/" (three spaces and a slash)
                    Document doc = new Document();

                    doc.add(new StringField("DocIDNPL", docIDNPL, Field.Store.YES));

                    doc.add(new TextField("Contents", contents.toString().trim(), Field.Store.YES));

                    if (demoEmbeddings != null) {
                        try (InputStream in = new ByteArrayInputStream(contents.toString().trim().getBytes(StandardCharsets.UTF_8))) {
                            float[] vector = demoEmbeddings.computeEmbedding(new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8)));
                            doc.add(new KnnVectorField("contents-vector", vector, VectorSimilarityFunction.DOT_PRODUCT));
                        }
                    }

                    if (writer.getConfig().getOpenMode() == OpenMode.CREATE) {
                        // New index, so we just add the document (no old document can be there):
                        System.out.println("adding " + doc.get("DocIDNPL"));
                        writer.addDocument(doc);
                    } else {
                        // Existing index (an old copy of this document may have been indexed) so
                        // we use updateDocument instead to replace the old one matching the exact
                        // DocIDNPL, if present:
                        System.out.println("updating " + doc.get("DocIDNPL"));
                        writer.updateDocument(new Term("DocIDNPL", docIDNPL), doc);
                    }
                    docIDNPL = null;
                    contents = new StringBuilder();
                } else {
                    // Append content to contentBuilder
                    contents.append(line).append(System.lineSeparator());
                }
            }
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

    private static Query addSemanticQuery(Query query, KnnVectorDict vectorDict, int k)
            throws IOException {
        StringBuilder semanticQueryText = new StringBuilder();
        QueryFieldTermExtractor termExtractor = new QueryFieldTermExtractor("contents");
        query.visit(termExtractor);
        for (String term : termExtractor.terms) {
            semanticQueryText.append(term).append(' ');
        }
        if (semanticQueryText.length() > 0) {
            KnnVectorQuery knnQuery =
                    new KnnVectorQuery(
                            "contents-vector",
                            new DemoEmbeddings(vectorDict).computeEmbedding(semanticQueryText.toString()),
                            k);
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            builder.add(query, BooleanClause.Occur.SHOULD);
            builder.add(knnQuery, BooleanClause.Occur.SHOULD);
            return builder.build();
        }
        return query;
    }

    private static class QueryFieldTermExtractor extends QueryVisitor {
        private final String field;
        private final List<String> terms = new ArrayList<>();

        QueryFieldTermExtractor(String field) {
            this.field = field;
        }

        @Override
        public boolean acceptField(String field) {
            return field.equals(this.field);
        }

        @Override
        public void consumeTerms(Query query, Term... terms) {
            for (Term term : terms) {
                this.terms.add(term.text());
            }
        }

        @Override
        public QueryVisitor getSubVisitor(BooleanClause.Occur occur, Query parent) {
            if (occur == BooleanClause.Occur.MUST_NOT) {
                return QueryVisitor.EMPTY_VISITOR;
            }
            return this;
        }
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(vectorDict);
    }
}