package org.dspace.ctask.general;

import org.apache.log4j.Logger;
import org.dspace.authorize.AuthorizeException;
import org.dspace.content.*;
import org.dspace.core.ConfigurationManager;
import org.dspace.curate.AbstractCurationTask;
import org.dspace.curate.Curator;
import org.dspace.curate.Distributive;

import java.io.*;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;

/**
 * User: cknowles - University of Edinburgh
 * Date: 14/04/15
 * Time: 14:39
 */
@Distributive
public class LoadTagsForBitstreams extends AbstractCurationTask {

    private static final String PLUGIN_PREFIX = "loadtags";

    // The status of the link checking of this item
    private int status = Curator.CURATE_UNSET;

    //loaded from config file in init
    private static String tagsFile = null;
    private static String stopWordsFile = null;
    private static String metadataSchema = null;
    private static String metadataElement = null;
    private static String metadataQualifier = null;
    private static String language = null;
    private Map<String, HashSet<String>> bitstreamMap = new HashMap<String, HashSet<String>>();
    private ArrayList<String> stopwords = new ArrayList<String>();

    // The log4j logger for this class
    private static Logger log = Logger.getLogger(LoadTagsForBitstreams.class);

    @Override
    public void init(Curator curator, String taskId) throws IOException {
        super.init(curator, taskId);

        tagsFile = ConfigurationManager.getProperty(PLUGIN_PREFIX, "tagsFile");
        stopWordsFile = ConfigurationManager.getProperty(PLUGIN_PREFIX, "stopFile");
        metadataSchema = ConfigurationManager.getProperty(PLUGIN_PREFIX, "metadataSchema");
        metadataElement = ConfigurationManager.getProperty(PLUGIN_PREFIX, "metadataElement");
        metadataQualifier = ConfigurationManager.getProperty(PLUGIN_PREFIX, "metadataQualifier");
        language = ConfigurationManager.getProperty(PLUGIN_PREFIX, "language");
    }

    @Override
    public int perform(DSpaceObject dso) throws IOException {
        StringBuilder results = new StringBuilder();

//if dso item run on this item
        status = Curator.CURATE_SKIP;
        try {


            loadStopWordsFile();
            loadTagFile();

            if (dso instanceof Item) {

                log.info("Using item " + dso.getHandle());
                Item item = (Item) dso;
                findBitstreams(results, item);
                //DSpace does not allow you to get all metadata for an item
            } else if (dso instanceof Collection) {
                log.info("Using Collection " + dso.getHandle());
                Collection collection = (Collection) dso;
                ItemIterator items = collection.getAllItems();
                while (items.hasNext()) {
                    Item item = items.next();
                    findBitstreams(results, item);
                }

            } else if (dso instanceof Community) {
                log.info("Using Community " + dso.getHandle());
                Community community = (Community) dso;
                Collection[] collections = community.getCollections();
                for (Collection collection : collections) {
                    ItemIterator items = collection.getAllItems();
                    while (items.hasNext()) {
                        Item item = items.next();
                        findBitstreams(results, item);
                    }
                }

            } else {
                log.info("dso is not an item, collection or community");
                results.append("DSpace Object is not an item, collection or community - SKIP\n");
            }
        } catch (SQLException se) {
            log.error(se.getMessage());
        }

        setResult(results.toString());
        report(results.toString());
        System.out.println(results.toString());
        return status;
    }

    private void findBitstreams(StringBuilder results, Item item) throws SQLException {

        ArrayList<String> filesLoaded = new ArrayList<String>();
        Bundle[] bundles = item.getBundles("ORIGINAL");
        for (Bundle bundle : bundles) {
            Bitstream[] bitstreams = bundle.getBitstreams();
            for (Bitstream bitstream : bitstreams) {
                //Edinburgh specific due to use of d and c in filenames for derivatives of master image
                String filenameWithout = bitstream.getName().replace("d","");
                filenameWithout = filenameWithout.replace("c","");
                filenameWithout = filenameWithout.replace(".jpg","");
                if (filenameWithout.indexOf("-") > 0){
                    filenameWithout = filenameWithout.substring(0, filenameWithout.indexOf("-"));
                }
                System.out.println(filenameWithout);
                //todo check that tags have not already been loaded for this item - bitstream combination

                if (bitstreamMap.containsKey(filenameWithout) && !filesLoaded.contains(filenameWithout)) {
                    //TODO load tags
                    filesLoaded.add(filenameWithout);
                    HashSet<String> tags = bitstreamMap.get(filenameWithout);
                    loadTags(results, item, tags);
                }

            }
        }
    }


    private void loadTags(StringBuilder results, Item item, HashSet<String> tags)
    {
        try
        {
            String [] tagsArray = tags.toArray(new String[tags.size()]);
            item.addMetadata(metadataSchema, metadataElement, metadataQualifier, language, tagsArray);

            item.update();
            System.out.println("Added tags to item " + item.getHandle());
            results.append("Added tags to item " + item.getHandle() +
                    " field " + metadataSchema + "." + metadataElement + "." + metadataQualifier +
                    " to item + " + item.getHandle() + " " + tags + "\n");
            status = Curator.CURATE_SUCCESS;

        } catch (AuthorizeException ae) {
            // Something went wrong
            logDebugMessage(ae.getMessage());
            System.out.println(ae);
            status = Curator.CURATE_ERROR;
        } catch (SQLException sqle) {
            // Something went wrong
            logDebugMessage(sqle.getMessage());
            System.out.println(sqle);
            status = Curator.CURATE_ERROR;
        }
    }

    /**
     * Debugging logging if required
     *
     * @param message The message to log
     */
    private void logDebugMessage(String message)
    {
        if (log.isDebugEnabled())
        {
            log.debug(message);
        }
    }

    private void loadTagFile()
    {

        BufferedReader fileReader = null;

        //Delimiter used in CSV file
        final String DELIMITER = ",";
        try
        {
            String line = "";
            //Create the file reader
            fileReader = new BufferedReader(new FileReader(tagsFile));

            //Read the file line by line
            while ((line = fileReader.readLine()) != null)
            {
                //Get all tokens available in line
                String[] tokens = line.split(DELIMITER);
                String key = tokens[0];
                key = key.replace("d","");
                key = key.replace("c", "");
                key = key.replace(".jpg","");
                if (key.indexOf("-") > 0){
                    key = key.substring(0, key.indexOf("-"));
                }
                HashSet<String> values = new HashSet<String>();
                for (int i = 1; i < tokens.length; i++) {
                    String token = tokens[i];
                    if (!token.isEmpty())
                    {
                        String tokenLower = token.toLowerCase();
                        if (!stopwords.contains(tokenLower))
                        {
                            values.add(token);
                        }
                        //else{
                        //    System.out.println("STOP " + token);
                        //}
                    }
                    //Print all tokens
                    //System.out.println(token);
                }
                if (!values.isEmpty())
                {
                    if (bitstreamMap.containsKey(key))
                    {
                        HashSet<String> existingValues = bitstreamMap.get(key);
                        values.addAll(existingValues);
                    }

                    bitstreamMap.put(key, values);
                    //System.out.println("key" + key + " values " + values.size());
                }
                //System.out.println(bitstreamMap.size());
            }
        }
        catch (Exception e) {
            e.printStackTrace();
            System.out.println(e);
        }
        finally
        {
            try {
                fileReader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void loadStopWordsFile()
    {
        BufferedReader fileReader = null;

        //Delimiter used in CSV file
        final String DELIMITER = ",";

        try
        {
            String line = "";
            //Create the file reader
            System.out.println("stop words file " + stopwords);
            fileReader = new BufferedReader(new FileReader(stopWordsFile));


            //Read the file line by line
            while ((line = fileReader.readLine()) != null)
            {
                //Get all tokens available in line
                String[] tokens = line.split(DELIMITER);

                for (String token : tokens) {
                    if (!token.isEmpty()) {
                        stopwords.add(token);
                    }
                }
            }

        }
        catch (Exception e) {
            e.printStackTrace();
            System.out.println(e);
        }
        finally
        {
            try {
                fileReader.close();
            }
            catch (NullPointerException e) {
                e.printStackTrace();
            }  catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}