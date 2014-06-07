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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.JarURLConnection;
import java.net.URL;
import java.util.jar.JarFile;

import junit.framework.TestResult;
import junit.framework.TestSuite;

import org.openrdf.OpenRDFUtil;
import org.openrdf.model.Resource;
import org.openrdf.model.ValueFactory;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.util.RDFInserter;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.helpers.BasicParserSettings;
import org.openrdf.rio.turtle.TurtleParser;
import org.openrdf.sail.memory.MemoryStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import info.aduna.io.FileUtil;
import info.aduna.io.ZipUtil;

public class ManifestTest
{
   private static final Logger LOG = LoggerFactory.getLogger(ManifestTest.class);

   public static TestSuite suite(R2RmlBaseTest.Factory factory) throws Exception
   {
      final String manifestUrl;
      final File tempDir;
      
      URL url = ManifestTest.class.getResource("/res/manifest-evaluation.ttl");
      
      if ("jar".equals(url.getProtocol())) {
         // Extract manifest files to a temporary directory
         try {
            tempDir = FileUtil.createTempDir("test-resources");
            
            JarURLConnection jarconn = (JarURLConnection) url.openConnection();
            JarFile jar = jarconn.getJarFile();
            
            ZipUtil.extract(jar, tempDir);
            
            File localFile = new File(tempDir, jarconn.getEntryName());
            manifestUrl = localFile.toURI().toURL().toString();
         }
         catch (IOException e) {
            throw new AssertionError(e);
         }
      }
      else {
         manifestUrl = url.toString();
         tempDir = null;
      }
      
      SailRepository repository = new SailRepository(new MemoryStore());
      repository.initialize();
      RepositoryConnection conn = repository.getConnection();
      
      addTurtle(conn, new URL(manifestUrl), manifestUrl);
      
      String query = 
            "SELECT DISTINCT manifestFile\n"
            + "FROM {x} rdf:first {manifestFile}\n"
            + "USING NAMESPACE\n"
            + "  mf = <http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#>";
      
      TestSuite suite = new TestSuite(factory.getClass().getName())
      {
         @Override
         public void run(TestResult result)
         {
            try {
               super.run(result);
            }
            finally {
               if (tempDir != null) {
                  try {
                     FileUtil.deleteDir(tempDir);
                  }
                  catch (IOException e) {
                     LOG.error("Unable to clean up temporary directory '" + tempDir + "': " + e.getMessage());
                  }
               }
            }
         }
      };
      
      TupleQueryResult results = conn.prepareTupleQuery(QueryLanguage.SERQL, query, manifestUrl).evaluate();
      while (results.hasNext()) {
         BindingSet bindingSet = results.next();
         String subManifestFile = bindingSet.getValue("manifestFile").stringValue();
         suite.addTest(R2RmlBaseTest.suite(subManifestFile, factory));
      }
      
      results.close();
      conn.close();
      repository.shutDown();
      
      LOG.info("Created aggregated test suite with " + suite.countTestCases() + " test cases.\n");
      
      return suite;
   }

   static void addTurtle(RepositoryConnection conn, URL documentUrl, String baseIri, Resource... contexts)
         throws IOException, RepositoryException, RDFParseException, RDFHandlerException
   {
      OpenRDFUtil.verifyContextNotNull(contexts);
      
      if (baseIri == null) {
         baseIri = documentUrl.toExternalForm();
      }
      
      InputStream is = documentUrl.openStream();
      try {
         final ValueFactory vf = conn.getRepository().getValueFactory();
         TurtleParser parser = new TurtleParser();
         parser.setValueFactory(vf);
         parser.getParserConfig().addNonFatalError(BasicParserSettings.FAIL_ON_UNKNOWN_DATATYPES);
         parser.getParserConfig().addNonFatalError(BasicParserSettings.VERIFY_DATATYPE_VALUES);
         parser.getParserConfig().addNonFatalError(BasicParserSettings.NORMALIZE_DATATYPE_VALUES);
         
         RDFInserter inserter = new RDFInserter(conn);
         inserter.enforceContext(contexts);
         parser.setRDFHandler(inserter);
         
         conn.begin();
         try {
            parser.parse(is, baseIri);
            conn.commit();
         }
         catch (RDFHandlerException e) {
            if (conn.isActive()) {
               conn.rollback();
            }
            if (e.getCause() != null && e.getCause() instanceof RepositoryException) {
               // RDFInserter only throws wrapped RepositoryExceptions
               throw (RepositoryException) e.getCause();
            }
            else {
               throw e;
            }
         }
         catch (RuntimeException e) {
            if (conn.isActive()) {
               conn.rollback();
            }
            throw e;
         }
      }
      finally {
         is.close();
      }
   }
}
