
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
import org.apache.lucene.analysis.core.*;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.es.SpanishAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.demo.knn.DemoEmbeddings;
import org.apache.lucene.demo.knn.KnnVectorDict;
import org.apache.lucene.document.*;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorSimilarityFunction;
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
public class IndexNPL implements AutoCloseable {
  static final String KNN_DICT = "knn-dict";

  // Calculates embedding vectors for KnnVector search
  private final DemoEmbeddings demoEmbeddings;
  private final KnnVectorDict vectorDict;

  private IndexNPL(KnnVectorDict vectorDict) throws IOException {
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
    String usage = "java es.udc.fi.ri.mrisearcher.IndexNPL"
            + " [-openmode OPEN_MODE] [-index INDEX_PATH] -docs DOCS_PATH [-analyzer ANALYZER] [-stopwords STOPWORDS_PATH] -indexingmodel jm LAMBDA_VALUE | dir MU_VALUE\n\n"
            + "This indexes the documents in DOCS_PATH using the specified analyzer and similarity model,"
            + "creating a Lucene index in INDEX_PATH that can be searched with the specified model.\n"
            + "Supported analyzers: standard, simple, whitespace, keyword, english\n"
            + "Supported similarity models: jm lambda, dir mu";
    String indexPath = "index";
    String docsPath = null;
    String openmode = "create_or_append";
    String analyzer = "standard";
    String stopwordsPath = null;
    boolean indexingmodel = false;
    float jmLambda = -1;
    float dirMu = -1;
    String vectorDictSource = null;
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
          String modelType = args[++i];
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
        default:
          throw new IllegalArgumentException("Unknown parameter: " + args[i]);
      }
    }

    if (docsPath == null || !indexingmodel) {
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

    File outFile = new File(indexPath+"-analyzer");
    PrintWriter writer2 = new PrintWriter(new FileWriter(outFile, false));
    writer2.println(analyzer);
    writer2.println(stopwordsPath);
    writer2.close();


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
      if (vectorDictSource != null) {
        KnnVectorDict.build(Paths.get(vectorDictSource), dir, KNN_DICT);
        vectorDictInstance = new KnnVectorDict(dir, KNN_DICT);
        vectorDictSize = vectorDictInstance.ramBytesUsed();
      }

      try (IndexWriter writer = new IndexWriter(dir, iwc);
           IndexNPL indexFiles = new IndexNPL(vectorDictInstance)) {
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

  @Override
  public void close() throws IOException {
    IOUtils.close(vectorDict);
  }
}

