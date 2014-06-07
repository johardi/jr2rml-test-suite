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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
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
import org.openrdf.rio.ntriples.NTriplesParser;
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

   private String mTestIri;
   private String mTestId;
   private String mTestTitle;
   private String mTestOutput;
   private String mMappingFile;
   private String mSqlScriptFile;
   private boolean bHasExpectedOutput;

   private boolean bTestSucceed = true;

   private Connection mDbConn;

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

   /**
    * Returns test IRI which is a unique codification ID for each
    * <code>rdb2rdftest:R2RML</code> test.
    */
   protected String getTestIri()
   {
      return mTestIri;
   }

   /**
    * Returns test identifier as specified by <code>dcterms:identifier</code> in
    * the manifest file.
    */
   protected String getTestId()
   {
      return mTestId;
   }

   /**
    * Returns test title as specified by <code>dcterms:title</code> in the
    * manifest file.
    */
   protected String getTestTitle()
   {
      return mTestTitle;
   }

   /**
    * Returns test output file as specified by <code>rdb2rdftest:output</code>
    * in the manifest file. It includes the full path of the file location.
    * 
    * The method will return an empty string if {@link hasExpectedOutput()}
    * returns <code>false</code>.
    */
   protected String getTestOutput()
   {
      return mTestOutput;
   }

   /**
    * Returns test mapping document as specified by
    * <code>db2rdftest:mappingDocument</code> in the manifest file. It includes
    * the full path of the file location.
    */
   protected String getMappingFile()
   {
      return mMappingFile;
   }

   /**
    * Returns SQL script file for table setup as specified by
    * <code>rdb2rdftest:sqlScriptFile</code> in the manifest file. It includes
    * the full path of the file location.
    */
   protected String getSqlScriptFile()
   {
      return mSqlScriptFile;
   }

   /**
    * Returns test output indicator as specified by
    * <code>rdb2rdftest:hasExpectedOutput</code> in the manifest file.
    */
   protected boolean hasExpectedOutput()
   {
      return bHasExpectedOutput;
   }

   @Override
   protected void setUp() throws Exception
   {
      LOG.info("Running {} test case", getTestId());
      
      java.sql.Statement stmt = null;
      try {
         Class.forName(getJdbcDriver());
         mDbConn = DriverManager.getConnection(getJdbcUrl(), getDbUser(), getDbPassword());
         stmt = mDbConn.createStatement();
         
         LOG.info("Creating tables and inserting data...");
         String sqlCreateTable = readSqlScript();
         stmt.executeUpdate(sqlCreateTable);
      }
      catch (SQLException e) {
         LOG.error(e.getMessage());
         bTestSucceed = false;
      }
      finally {
         if (stmt != null && !stmt.isClosed()) {
            stmt.close();
         }
      }
   }

   @Override
   protected void runTest() throws Exception
   {
      try {
         runProcessor();
         Set<Statement> actualResult = getActualGraph();
         Set<Statement> expectedResult = getExpectedGraph();
         compareGraphs(actualResult, expectedResult);
      }
      catch (Exception e) {
         cleanDatabase();
         bTestSucceed = false;
         fail(e.getMessage());
      }
   }

   @Override
   protected void tearDown() throws Exception
   {
      cleanDatabase();
      printTestStatus();
   }

   /**
    * Returns the JDBC driver name
    */
   protected abstract String getJdbcDriver();

   /**
    * Returns the JDBC connection string URL
    */
   protected abstract String getJdbcUrl();

   /**
    * Returns the database user name with CREATE TABLE permission enabled
    */
   protected abstract String getDbUser();

   /**
    * Returns the database password for the associated user name.
    */
   protected abstract String getDbPassword();

   /**
    * Runs the R2RML processing action and generates RDF triple graphs. Later
    * the result will be compared to the expected output by overriding the
    * method {@link getActualGraph()}.
    */
   protected abstract void runProcessor() throws Exception;

   /**
    * Returns the graph statements from executing the R2RML processor. The
    * execution must be performed by overriding method {@link runProcessor()}.
    */
   protected abstract Set<Statement> getActualGraph() throws Exception;

   /**
    * Creates a test suite that contains all test cases mentioned in the
    * <code>manifest.ttl</code> file, including those tests that are not
    * reviewed.
    *
    * @param manifestUrl
    *           The <code>manifest.ttl</code> file location.
    * @param factory
    *           An object factory to create this class instance.
    * @return A test suite.
    */
   public static TestSuite suite(String manifestUrl, Factory factory) throws Exception
   {
      return suite(manifestUrl, factory, false);
   }

   /**
    * Creates a test suite that contains all test cases mentioned in the
    * <code>manifest.ttl</code> file.
    * 
    * @param manifestUrl
    *           The <code>manifest.ttl</code> file location.
    * @param factory
    *           An object factory to create this class instance.
    * @param onlyReviewedTests
    *           To filter whether un-reviewed test should be included in the
    *           test suite or not.
    * @return a test suite.
    */
   public static TestSuite suite(String manifestUrl, Factory factory, boolean onlyReviewedTests) throws Exception
   {
      String manifestRootPath = manifestUrl.substring(0, manifestUrl.lastIndexOf("/") + 1);
      
      Repository repo = createNewRepository();
      RepositoryConnection conn = repo.getConnection();
      ManifestTest.addTurtle(conn, new URL(manifestUrl), "http://example.com/base/");
      
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

   /*
    * Private utility methods
    */

   private String readSqlScript() throws IOException
   {
      StringBuilder sql = new StringBuilder();
      
      URL sqlScriptUrl = new URL(mSqlScriptFile);
      BufferedReader br = new BufferedReader(new InputStreamReader(sqlScriptUrl.openStream()));
      try {
         String line = "";
         boolean needNewline = false;
         while ((line = br.readLine()) != null) {
            if (needNewline) {
               sql.append(System.lineSeparator());
            }
            sql.append(line);
            needNewline = true;
         }
         return sql.toString();
      }
      finally {
         if (br != null) {
            br.close();
         }
      }
   }

   private final Set<Statement> getExpectedGraph() throws Exception
   {
      NTriplesParser parser = new NTriplesParser();
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

   private final void compareGraphs(Set<Statement> actual, Set<Statement> expected) throws Exception
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
         
         bTestSucceed = false;
         LOG.error(message.toString());
         fail(message.toString());
      }
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

   private void cleanDatabase() throws Exception
   {
      if (mDbConn.isClosed()) {
         return;
      }
      java.sql.Statement stmt = mDbConn.createStatement();
      try {
         LOG.info("Dropping tables...");
         List<String> tableNames = getTableNames();
         for (String tableName : tableNames) {
            String sqlDropTable = String.format("DROP TABLE \"%s\"", tableName);
            stmt.executeUpdate(sqlDropTable);
         }
      }
      catch (SQLException e) {
         LOG.error(e.getMessage());
      }
      finally {
         if (stmt != null && !stmt.isClosed()) {
            stmt.close();
         }
         if (mDbConn != null && !mDbConn.isClosed()) {
            mDbConn.close();
         }
      }
   }

   private List<String> getTableNames() throws SQLException
   {
      List<String> toReturn = new ArrayList<String>();
      DatabaseMetaData meta = mDbConn.getMetaData();
      ResultSet rs = meta.getTables(null, null, "%", new String[] {"TABLE"});
      try {
         while (rs.next()) {
            String tableName = rs.getString("TABLE_NAME");
            if (hasReference(tableName, meta)) {
               // Give a higher priority for table that has FK
               toReturn.add(0, tableName);
            }
            else {
               toReturn.add(tableName);
            }
         }
      }
      finally {
         if (rs != null && !rs.isClosed()) {
            rs.close();
         }
      }
      return toReturn;
   }

   private boolean hasReference(String tableName, DatabaseMetaData meta) throws SQLException
   {
      ResultSet rs = null;
      boolean toReturn = false;
      try {
         rs = meta.getImportedKeys(null, null, tableName);
         toReturn = rs.next();
      }
      finally {
         if (rs != null && !rs.isClosed()) {
            rs.close();
         }
      }
      return toReturn;
   }

   private void printTestStatus()
   {
      if (bTestSucceed) {
         LOG.info("Test OK.\n");
      }
      else {
         LOG.info("Test FAILED.\n");
      }
   }
}
