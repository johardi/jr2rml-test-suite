/*
 * JR2RmlTestSuite is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * JR2RmlTestSuite is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with JR2RmlTestSuite. If not, see http://www.gnu.org/licenses/.
 * 
 * Contributors:
 *     Josef Hardi <josef.hardi@gmail.com> - initial API and implementation
 */
package io.github.johardi.r2rml.testsuite;

import java.io.InputStream;
import java.net.URL;
import java.util.LinkedHashSet;
import java.util.Set;

import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.openrdf.model.Statement;
import org.openrdf.model.Value;
import org.openrdf.model.util.ModelUtil;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.helpers.BasicParserSettings;
import org.openrdf.rio.helpers.StatementCollector;
import org.openrdf.rio.turtle.TurtleParser;
import org.openrdf.sail.memory.MemoryStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import info.aduna.text.StringUtil;

public abstract class R2RmlBaseTest extends TestCase
{
   public interface Factory
   {
      R2RmlBaseTest createR2RmlTest(
            String testIri,               // test object identifier
            String testId,                // as specified in dcterms:identifier
            String testTitle,             // as specified in dcterms:title
            String testOutput,            // as specified in rdb2rdftest:output
            String mappingFile,           // as specified in rdb2rdftest:mappingDocument
            String sqlScriptFile,         // as specified in rdb2rdftest:sqlScriptFile
            boolean hasExpectedOutput);   // as specified in rdb2rdftest:hasExpectedOutput
   }

   protected String mTestIri;
   protected String mTestId;
   protected String mTestTitle;
   protected String mTestOutput;
   protected String mMappingFile;
   protected String mSqlScriptFile;
   protected boolean bHasExpectedOutput;

   private static final Logger LOG = LoggerFactory.getLogger(R2RmlBaseTest.class);

   public R2RmlBaseTest(String testIri, String testId, String testTitle, String testOutput,
         String mappingFile, String sqlScriptFile, boolean hasExpectedOutput)
   {
      super(testId);
      mTestIri = testIri;
      mTestId = testId;
      mTestTitle = testTitle;
      mTestOutput = testOutput;
      mMappingFile = mappingFile;
      mSqlScriptFile = sqlScriptFile;
      bHasExpectedOutput = hasExpectedOutput;
   }

   @Override
   protected void runTest() throws Exception
   {
      Set<Statement> actualResult = getActualGraph();
      Set<Statement> expectedResult = getExpectedGraph();
      compareGraphs(actualResult, expectedResult);
   }

   protected abstract Set<Statement> getActualGraph() throws Exception;

   protected final Set<Statement> getExpectedGraph() throws Exception
   {
      TurtleParser parser = new TurtleParser();
      parser.getParserConfig().addNonFatalError(BasicParserSettings.FAIL_ON_UNKNOWN_DATATYPES);
      parser.getParserConfig().addNonFatalError(BasicParserSettings.VERIFY_DATATYPE_VALUES);
      parser.getParserConfig().addNonFatalError(BasicParserSettings.NORMALIZE_DATATYPE_VALUES);
      parser.setPreserveBNodeIDs(true);
      
      Set<Statement> result = new LinkedHashSet<Statement>();
      parser.setRDFHandler(new StatementCollector(result));
      
      InputStream in = new URL(mTestOutput).openStream();
      try {
         parser.parse(in, mTestOutput);
      }
      finally {
         in.close();
      }
      return result;
   }

   protected final void compareGraphs(Set<Statement> actual, Set<Statement> expected) throws Exception
   {
      if (!ModelUtil.equals(expected, actual)) {
         StringBuilder message = new StringBuilder(128);
         message.append("\n=============== ").append(getName()).append(" =======================\n");
         message.append("Expected result: \n");
         for (Statement st : expected) {
            message.append(st.toString());
            message.append("\n");
         }
         message.append("=============");
         StringUtil.appendN('=', getName().length(), message);
         message.append("========================\n");
         
         message.append("Actual result: \n");
         for (Statement st : actual) {
            message.append(st.toString());
            message.append("\n");
         }
         message.append("=============");
         StringUtil.appendN('=', getName().length(), message);
         message.append("========================\n");
         
         LOG.error(message.toString());
         fail(message.toString());
      }
   }

   public static TestSuite suite(String manifestUrl, Factory factory) throws Exception
   {
      return suite(manifestUrl, factory, false);
   }

   public static TestSuite suite(String manifestUrl, Factory factory, boolean onlyReviewedTests) throws Exception
   {
      String manifestRootPath = manifestUrl.substring(0, manifestUrl.lastIndexOf("/") + 1);
      
      Repository repo = createNewRepository();
      RepositoryConnection conn = repo.getConnection();
      ManifestTest.addTurtle(conn, new URL(manifestUrl), manifestUrl);
      
      String manifestId = getManifestName(repo, conn, manifestUrl);
      String sqlScriptFile = getSqlScriptFile(repo, conn, manifestUrl);
      
      TestSuite suite = new TestSuite(factory.getClass().getName());
      suite.setName(manifestId);
      
      LOG.info("Building test suite for {}", manifestId);
      
      StringBuilder query = new StringBuilder(512);
      query.append("SELECT DISTINCT testIri, testId, testTitle, testOutput, mappingFile, hasExpectedOutput").append("\n");
      query.append("FROM {testIri} rdf:type {rdb2rdftest:R2RML};").append("\n");
      query.append("               dcterms:identifier {testId}; ").append("\n");
      query.append("               dcterms:title {testTitle};").append("\n");
      query.append("               rdb2rdftest:output {testOutput};").append("\n");
      query.append("               rdb2rdftest:mappingDocument {mappingFile};").append("\n");
      query.append("               rdb2rdftest:hasExpectedOutput {hasExpectedOutput};").append("\n");
      query.append("               test:reviewStatus {testStatus}").append("\n");
      if (onlyReviewedTests) {
         query.append("WHERE testStatus = true").append("\n");
      }
      query.append("USING NAMESPACE").append("\n");
      query.append("   test = <http://www.w3.org/2006/03/test-description#>,").append("\n");
      query.append("   dcterms = <http://purl.org/dc/elements/1.1/>,").append("\n");
      query.append("   rdb2rdftest = <http://purl.org/NET/rdb2rdf-test#>");
      
      TupleQueryResult results = conn.prepareTupleQuery(QueryLanguage.SERQL, query.toString()).evaluate();
      while (results.hasNext()) {
         BindingSet bindingSet = results.next();
         R2RmlBaseTest testCase = factory.createR2RmlTest(
               getString(bindingSet.getValue("testIri")),
               getString(bindingSet.getValue("testId")),
               getString(bindingSet.getValue("testTitle")),
               manifestRootPath + getString(bindingSet.getValue("testOutput")),
               manifestRootPath + getString(bindingSet.getValue("mappingFile")),
               manifestRootPath + sqlScriptFile,
               Boolean.parseBoolean(getString(bindingSet.getValue("hasExpectedOutput"))));
         suite.addTest(testCase);
      }
      results.close();
      conn.close();
      repo.shutDown();
      
      LOG.info("Created test suite with " + suite.countTestCases() + " test cases.");
      return suite;
   }

   private static Repository createNewRepository() throws RepositoryException
   {
      Repository repo = new SailRepository(new MemoryStore());
      repo.initialize();
      return repo;
   }

   private static String getString(Value value)
   {
      if (value != null) {
         return value.stringValue();
      }
      return "";
   }

   private static String getManifestName(Repository repo, RepositoryConnection conn, String manifestUrl)
         throws QueryEvaluationException, RepositoryException, MalformedQueryException
   {
      String query =
            "SELECT manifestName\n"
            + "FROM {} rdf:type {rdb2rdftest:DataBase};\n"
            + "        dcterms:identifier {manifestName}\n"
            + "USING NAMESPACE\n"
            + "   dcterms = <http://purl.org/dc/elements/1.1/>,\n"
            + "   rdb2rdftest = <http://purl.org/NET/rdb2rdf-test#>";
      
      TupleQueryResult results = conn.prepareTupleQuery(QueryLanguage.SERQL, query).evaluate();
      try {
         if (results.hasNext()) {
            return results.next().getValue("manifestName").stringValue();
         }
      }
      finally {
         results.close();
      }
      
      // Derive name from manifest URL
      int lastSlashIdx = manifestUrl.lastIndexOf('/');
      int secLastSlashIdx = manifestUrl.lastIndexOf('/', lastSlashIdx - 1);
      return manifestUrl.substring(secLastSlashIdx + 1, lastSlashIdx);
   }

   private static String getSqlScriptFile(Repository repo, RepositoryConnection conn, String manifestUrl) 
         throws QueryEvaluationException, RepositoryException, MalformedQueryException
   {
      String query =
            "SELECT sqlScriptFile\n"
            + "FROM {} rdf:type {rdb2rdftest:DataBase};\n"
            + "        rdb2rdftest:sqlScriptFile {sqlScriptFile}\n"
            + "USING NAMESPACE\n"
            + "   rdb2rdftest = <http://purl.org/NET/rdb2rdf-test#>";
      
      TupleQueryResult results = conn.prepareTupleQuery(QueryLanguage.SERQL, query).evaluate();
      try {
         if (results.hasNext()) {
            return results.next().getValue("sqlScriptFile").stringValue();
         }
         throw new QueryEvaluationException("Missing SQL script file in the manifest");
      }
      finally {
         results.close();
      }
   }
}
