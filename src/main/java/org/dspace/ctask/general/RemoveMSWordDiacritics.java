package org.dspace.ctask.general;

import org.dspace.authorize.AuthorizeException;
import org.dspace.content.*;
import org.dspace.curate.AbstractCurationTask;
import org.dspace.curate.Curator;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import org.apache.log4j.Logger;


/**
 * User: cknowles - University of Edinburgh
 * Date: 07/02/13
 * Time: 14:39
 */
public class RemoveMSWordDiacritics extends AbstractCurationTask {

    // The results of link checking this item
    private List<String> results = null;

    // The status of the link checking of this item
    private int status = Curator.CURATE_UNSET;

    // The log4j logger for this class
    private static Logger log = Logger.getLogger(RemoveMSWordDiacritics.class);

    @Override
    public int perform(DSpaceObject dso) throws IOException {

        StringBuilder results = new StringBuilder();

        //if dso item run on this item
        status = Curator.CURATE_SKIP;
        try {
            if (dso instanceof Item) {

                log.info("Using item " + dso.getHandle());
                Item item = (Item) dso;
                //DSpace does not allow you to get all metadata for an item
                removeItemDiacritics(results, item);
            } else if (dso instanceof Collection) {
                log.info("Using Collection " + dso.getHandle());
                Collection collection = (Collection) dso;
                ItemIterator items = collection.getAllItems();
                while (items.hasNext()) {
                    Item item = items.next();
                    removeItemDiacritics(results, item);
                }

            } else if (dso instanceof Community) {
                log.info("Using Community " + dso.getHandle());
                Community community = (Community) dso;
                Collection[] collections = community.getCollections();
                for (Collection collection : collections) {
                      ItemIterator items = collection.getAllItems();
                      while (items.hasNext()) {
                          Item item = items.next();
                          removeItemDiacritics(results, item);
                      }

                }

            } else {
                log.info("dso is not an item, collection or community");
            }
        } catch (SQLException se) {
            log.error(se.getMessage());
        }


        setResult(results.toString());
        report(results.toString());
        System.out.println(results.toString());
        return status;
    }

    private void removeItemDiacritics(StringBuilder results, Item item) {
        DCValue[] dcValues = item.getMetadata("dc.title");
        log.info("dcValues " + dcValues.length);
        for (int i = 0; i < dcValues.length; i++) {
            DCValue dcValue = dcValues[i];
            status = removeDiacritics(item, dcValue, results);

        }
    }

    /**
     * Check the URL and perform appropriate reporting
     *
     * @param dcValue The dcValue to check
     * @return If the URL was OK or not
     */
    protected int removeDiacritics(Item item, DCValue dcValue, StringBuilder results) {
        String value = dcValue.value;
        //replace
        String newString = value.replaceAll("[\\u2018|\\u2019]", "'")
                .replaceAll("[\\u201C|\\u201D|\\u201E]", "\"")
                .replaceAll("[\\u2039|\\u203A]", "\\")
                .replaceAll("[\\u02DC]", "~")
                .replaceAll("[\\u2013|\\u2014]", "-");
        if (newString.equals(value)) {
            results.append("No updated required - SKIP\n");
            return Curator.CURATE_SKIP;
        } else {
            results.append(value + " = UPDATED TO = " + newString + " = SUCCESS\n");
            item.clearMetadata("dc", "title", Item.ANY, Item.ANY);
            item.addMetadata("dc", "title", null, "en", newString);
            try {
                item.update();
            } catch (AuthorizeException ae) {
                // Something went wrong
                log.debug(ae.getMessage());
                status = Curator.CURATE_ERROR;
            } catch (SQLException sqle) {
                // Something went wrong
                log.debug(sqle.getMessage());
                status = Curator.CURATE_ERROR;
            }

            return status;
        }
        //update item metadata with new String
        //if updated return success;


    }

}