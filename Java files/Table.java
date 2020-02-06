/****************************************************************************************
 * @file  Table.java
 *
 * @author   Kathryn Brown, Daniel Garcia, Matt Colley
 */

import java.io.*;
import java.util.*;
import java.util.function.*;
import java.util.stream.*;

// import static java.lang.Boolean.*;
import static java.lang.System.out;

/****************************************************************************************
 * ---THIS class implements relational DB tables
 * -  Attribute names, Domains, A list of tuples
 * ---FIVE basic relational algebra operators provided:
 * -  Project, Select, Union, Minus, and Join
 * ---INSERT data manipulation operator is also provided
 * -  Does not include update and delete data manipulation operators
 */
public class Table
       implements Serializable
{
    /** Relative path for storage directory. */
    private static final String DIR = "store" + File.separator;
    /** Filename extension for database files. */
    private static final String EXT = ".dbf";
    /** Counter for naming temporary tables. */
    private static int count = 0;
    /** Table name. */
    private final String name;
    /** Array of attribute names. */
    private final String [] attribute;
    /** Array of attribute domains: a domain may be
     *  INTEGER types: Long, Integer, Short, Byte
     *  REAL types: Double, Float
     *  STRING types: Character, String */
    private final Class [] domain;
    /** Collection of tuples (data storage). */
    private final List <Comparable []> tuples;
    /** Primary key(s). */
    private final String [] key;
    /** Index into tuples (maps key to tuple number). */
    private final Map <KeyType, Comparable []> index;

    //----------------------------------------------------------------------------------
    // Constructors
    //----------------------------------------------------------------------------------

    /************************************************************************************
     * Construct an empty table from the meta-data specifications.
     * @param _name       the name of the relation
     * @param _attribute  the string containing attributes names
     * @param _domain     the string containing attribute domains (data types)
     * @param _key        the primary key
     */  
    public Table (String _name, String [] _attribute, Class [] _domain, String [] _key)
    {
        name      = _name;
        attribute = _attribute;
        domain    = _domain;
        key       = _key;
        tuples    = new ArrayList <> ();
        index     = new TreeMap <> ();       // also try BPTreeMap, LinHashMap or ExtHashMap
        // index     = new LinHashMap <> (KeyType.class, Comparable [].class);

    } // constructor

    /************************************************************************************
     * Construct a table from the meta-data specifications and data in _tuples list.
     * @param _name       the name of the relation
     * @param _attribute  the string containing attributes names
     * @param _domain     the string containing attribute domains (data types)
     * @param _key        the primary key
     * @param _tuple      the list of tuples containing the data
     */  
    public Table (String _name, String [] _attribute, Class [] _domain, String [] _key,
                  List <Comparable []> _tuples)
    {
        name      = _name;
        attribute = _attribute;
        domain    = _domain;
        key       = _key;
        tuples    = _tuples;
        index     = new TreeMap <> ();       // also try BPTreeMap, LinHashMap or ExtHashMap
    } // constructor

    /************************************************************************************
     * Construct an empty table from the raw string specifications.
     * @param name        the name of the relation
     * @param attributes  the string containing attributes names
     * @param domains     the string containing attribute domains (data types)
     */
    public Table (String name, String attributes, String domains, String _key)
    {
        this (name, attributes.split (" "), findClass (domains.split (" ")), _key.split(" "));

        out.println ("DDL> create table " + name + " (" + attributes + ")");
    } // constructor

    //----------------------------------------------------------------------------------
    // Public Methods
    //----------------------------------------------------------------------------------

    /************************************************************************************
     * Project the tuples onto a lower dimension by keeping only the given attributes.
     * Check whether the original key is included in the projection.
     *
     * #usage movie.project ("title year studioNo")
     *
     * @augtor Kathryn Brown, 811519926
     * @param attributes  the attributes to project onto
     * @return  a table of projected tuples
     */
    public Table project (String attributes)
    {
        out.println ("RA> " + name + ".project (" + attributes + ")");
        String [] attrs     = attributes.split (" ");
        Class []  colDomain = extractDom (match (attrs), domain);
        String [] newKey    = (Arrays.asList (attrs).containsAll (Arrays.asList (key))) ? key : attrs;

        List <Comparable []> rows = new ArrayList <> ();

        //  K A T I E ' S  W O R K
        for (Comparable [] row : this.tuples){
            rows.add(this.extract(row, attrs));
        }
        //  K A T I E ' S  W O R K

        return new Table (name + count++, attrs, colDomain, newKey, rows);
    } // project

    /************************************************************************************
     * Select the tuples satisfying the given predicate (Boolean function).
     *
     * #usage movie.select (t -> t[movie.col("year")].equals (1977))
     *
     * @param predicate  the check condition for tuples
     * @return  a table with tuples satisfying the predicate
     */
    public Table select (Predicate <Comparable []> predicate)
    {
        out.println ("RA> " + name + ".select (" + predicate + ")");

        return new Table (name + count++, attribute, domain, key,
                   tuples.stream ().filter (t -> predicate.test (t))
                                   .collect (Collectors.toList ()));
    } // select

    /************************************************************************************
     * Select the tuples satisfying the given key predicate (key = value).  Use an index
     * (Map) to retrieve the tuple with the given key value.
     *
     * @author Kathryn Brown, 811519926
     * @param keyVal  the given key value
     * @return  a table with the tuple satisfying the key predicate
     */
    public Table select (KeyType keyVal)
    {
        out.println ("RA> " + name + ".select (" + keyVal + ")");

        List <Comparable []> rows = new ArrayList <> ();



        out.println("ARRAY LIST:\n" + rows);

        //  K A T I E ' S  W O R K
        if(index.containsKey(keyVal)){
            rows.add(index.get(keyVal));
        }
        /*
        for(Map.Entry <KeyType, Comparable []> e: index.entrySet()){
            if(e.getKey().equals(keyVal)){
                rows.add(e.getValue());
            }
        }
        */
        //  K A T I E ' S  W O R K

        return new Table (name + count++, attribute, domain, key, rows);
    } // select

    /************************************************************************************
     * Union this table and table2.  Check that the two tables are compatible.
     *
     * #usage movie.union (show)
     *
     * @author Matthew Colley, 811709135
     * @param table2  the rhs table in the union operation
     * @return  a table representing the union
     */
    public Table union (Table table2)
    {
        out.println ("RA> " + name + ".union (" + table2.name + ")");
        if (! compatible (table2)) return null;

        List <Comparable []> rows = new ArrayList <> ();

        //  T O   B E   I M P L E M E N T E D 

        return new Table (name + count++, attribute, domain, key, rows);
    } // union

    /************************************************************************************
     * Take the difference of this table and table2.  Check that the two tables are
     * compatible.
     *
     * #usage movie.minus (show)
     *
     * @author Matthew Colley, 811709135
     * @param table2  The rhs table in the minus operation
     * @return  a table representing the difference
     */
    public Table minus (Table table2)
    {
        out.println ("RA> " + name + ".minus (" + table2.name + ")");
        if (! compatible (table2)) return null;

        List <Comparable []> rows = new ArrayList <> ();

        //  T O   B E   I M P L E M E N T E D 

        return new Table (name + count++, attribute, domain, key, rows);
    } // minus

    /************************************************************************************
     * Join this table and table2 by performing an "equi-join".  Tuples from both tables
     * are compared requiring attributes1 to equal attributes2.  Disambiguate attribute
     * names by append "2" to the end of any duplicate attribute name.
     *
     * #usage movie.join ("studioNo", "name", studio)
     *
     * @author Daniel Garcia, 811885075
     * @param attribute1  the attributes of this table to be compared (Foreign Key)
     * @param attribute2  the attributes of table2 to be compared (Primary Key)
     * @param table2      the rhs table in the join operation
     * @return  a table with tuples satisfying the equality predicate
     */
    public Table join (String attributes1, String attributes2, Table table2)
    {
        out.println ("RA> " + name + ".join (" + attributes1 + ", " + attributes2 + ", "
                                               + table2.name + ")");

        String [] t_attrs = attributes1.split (" ");
        String [] u_attrs = attributes2.split (" ");

        List <Comparable []> rows = new ArrayList <> ();

        //gets the column positions of attributes t_attrs and u_attrs
        int[] att1ColPos = this.match(t_attrs);
        int[] att2ColPos = table2.match(u_attrs);

        //gets the domains for above columns
        Class[] table1Domains = this.extractDom(att1ColPos, this.domain);
        Class[] table2Domains = this.extractDom(att2ColPos, table2.domain);

        String[] table2Attributes = new String[table2.attribute.length];

        //if domains are different, nothing happens
        if (Arrays.equals(table1Domains, table2Domains)) {

            //for loops do Cartesian product of tuples of this table and table 2
            //by matching tuple values of specified attributes and puts new tuples in rows
            for (Comparable[] t1 : this.tuples) {
                for (Comparable[] t2 : table2.tuples) {
                    int matches = 0;

                    for (int i = 0; i < att1ColPos.length; i++) {
                        if (t1[(int)att1ColPos[i]].equals(t2[(int)att2ColPos[i]])) {
                            matches++;
                        }
                        if (i == att1ColPos.length - 1 && matches == att1ColPos.length) {
                            rows.add(ArrayUtil.concat(t1, t2));
                        }
                    }
                }
            }

            //disambiguates attribute names by appending "2" to the end of any duplicate
            //attribute name as suggested above
            for (int i = 0; i < table2.attribute.length; i++) {
                table2Attributes[i] = table2.attribute[i];
            }
            for (int att2 = 0; att2 < table2Attributes.length; att2++) {
                for (int att1 = 0; att1 < this.attribute.length; att1++) {
                    if (this.attribute[att1].equalsIgnoreCase(table2Attributes[att2])) {
                        table2Attributes[att2] = table2Attributes[att2] + "2";
                    }
                }
            }
        }

        return new Table (name + count++, ArrayUtil.concat (attribute, table2.attribute),
                                          ArrayUtil.concat (domain, table2.domain), key, rows);
    } // join

    /************************************************************************************
     * Join this table and table2 by performing an "natural join".  Tuples from both tables
     * are compared requiring common attributes to be equal.  The duplicate column is also
     * eliminated.
     *
     * #usage movieStar.join (starsIn)
     *
     * @author Daniel Garcia, 811885075
     * @param table2  the rhs table in the join operation
     * @return  a table with tuples satisfying the equality predicate
     */
    public Table join (Table table2)
    {
        out.println ("RA> " + name + ".join (" + table2.name + ")");

        List <Comparable []> rows = new ArrayList <> ();

        // for pulling attributes for tables
        Comparable[] table1Attributes = new Comparable[this.attribute.length];
        Comparable[] table2Attributes = new Comparable[table2.attribute.length];

        // for pulling positions based on attributes
        Comparable[] table1Positions = new Comparable[this.attribute.length];
        Comparable[] table2Positions = new Comparable[table2.attribute.length];

        // below actually fills in the above arrays
        int counter = 0;

        for (int i = 0; i < this.attribute.length; i++) {
            for (int j = 0; j < table2.attribute.length; j++) {
                if (this.attribute[i].equalsIgnoreCase(table2.attribute[j])) {
                    // fills arrays with "indexes" of positions
                    table1Positions[counter] = i;
                    table2Positions[counter] = j;

                    // fills arrays with attributes
                    table1Attributes[counter] = this.attribute[i];
                    table2Attributes[counter] = table2.attribute[j];

                    counter++;
                    break;
                }
            }
        }

        Comparable[] attrPosTable1 = new Comparable[counter];
        Comparable[] attrPosTable2 = new Comparable[counter];
        Comparable[] attrTable2 = new Comparable[counter];

        // fills new arrays with above arrays
        for (int i = 0; i < counter; i++) {
            attrPosTable1[i] = table1Positions[i];
            attrPosTable2[i] = table2Positions[i];
            attrTable2[i] = table2Attributes[i];
        }

        int nullCount = 0;

        // counts for nulls
        for (int i = 0; i < attrPosTable1.length; i++) {
            if (attrPosTable1[i] == null) {
                nullCount++;
            }
        }

        // compares tuples of the tables based on their matching attributes
        // and fills "rows" array with 'joined' tuples
        if (nullCount != attrPosTable1.length) {
            for (Comparable[] tup1 : this.tuples) {
                for (Comparable[] tup2 : table2.tuples) {
                    int countForMatchJoin = 0; // holds number of matching tuples

                    for (int i = 0; i < attrPosTable1.length; i++) {
                        if (tup1[(int) attrPosTable1[i]].equals(tup2[(int) attrPosTable2[i]])) {
                            countForMatchJoin++;
                        }
                    }

                    if (countForMatchJoin == attrPosTable1.length) {
                        rows.add(ArrayUtil.concat(tup1, tup2));
                    }
                }
            }

            // next block of code does resizing of attributes and domains of table 2
            // in order to avoid duplicate columns in end table

            List<String> newTableAttributesList = new ArrayList<>();
            List<Class> newTableDomainsList = new ArrayList<>();

            boolean contains = true;
            for (int i = 0; i < table2.attribute.length; i++) {
                for (int j = 0; j < attrTable2.length; j++) {
                    if (table2.attribute[i].equalsIgnoreCase((String) attrTable2[j])) {
                        contains = false;
                    }
                }

                if (contains == true) {
                    newTableAttributesList.add(table2.attribute[i]);
                    newTableDomainsList.add(table2.domain[i]);
                }
                contains = true;
            }

            String[] newTableAttributes = newTableAttributesList.toArray(new String[newTableAttributesList.size()]);
            Class[] newTableDomains = newTableDomainsList.toArray(new Class[newTableDomainsList.size()]);

            // "rows" is fixed to make sure it fits with resized values of attributes and
            // domains of table 2

            Comparable[] actualPos = new Comparable[this.attribute.length
                    + (table2.attribute.length - attrPosTable2.length)];
            int posIndex = 0;

            //for loop runs through tables to find accurate positions for attributes
            for (int i = 0; i < (this.attribute.length + table2.attribute.length); i++) {
                if (i < this.attribute.length) {
                    actualPos[posIndex] = i;
                    posIndex++;
                } else {
                    int countForNotMatch = 0;
                    for (int j = 0; j < attrPosTable2.length; j++) {
                        if (!(i == (int) attrPosTable2[j] + this.attribute.length)) {
                            countForNotMatch++;
                        }
                    }
                    if (countForNotMatch == attrPosTable2.length) {
                        actualPos[posIndex] = i;
                        posIndex++;
                    }
                }
            }

            for (int t = 0; t < rows.size(); t++) {
                Comparable[] newTup = new Comparable[actualPos.length];
                for (int i = 0; i < actualPos.length; i++) {
                    newTup[i] = rows.get(t)[(int) actualPos[i]];
                }
                rows.remove(rows.get(t));

                rows.add(t, newTup);
            }

            return new Table(name + counter++, ArrayUtil.concat(attribute, newTableAttributes),
                    ArrayUtil.concat(domain, newTableDomains), key, rows);
        }
        // if is empty
        else {
            return new Table(name + counter++, ArrayUtil.concat(attribute, table2.attribute),
                    ArrayUtil.concat(domain, table2.domain), key, rows);
        }
    } // join

    /************************************************************************************
     * Return the column position for the given attribute name.
     *
     * @param attr  the given attribute name
     * @return  a column position
     */
    public int col (String attr)
    {
        for (int i = 0; i < attribute.length; i++) {
           if (attr.equals (attribute [i])) return i;
        } // for

        return -1;  // not found
    } // col

    /************************************************************************************
     * Insert a tuple to the table.
     *
     * #usage movie.insert ("'Star_Wars'", 1977, 124, "T", "Fox", 12345)
     *
     * @param tup  the array of attribute values forming the tuple
     * @return  whether insertion was successful
     */
    public boolean insert (Comparable [] tup)
    {
        out.println ("DML> insert into " + name + " values ( " + Arrays.toString (tup) + " )");

        if (typeCheck (tup)) {
            tuples.add (tup);
            Comparable [] keyVal = new Comparable [key.length];
            int []        cols   = match (key);
            for (int j = 0; j < keyVal.length; j++) keyVal [j] = tup [cols [j]];
            index.put (new KeyType (keyVal), tup);
            return true;
        } else {
            return false;
        } // if
    } // insert

    /************************************************************************************
     * Get the name of the table.
     *
     * @return  the table's name
     */
    public String getName ()
    {
        return name;
    } // getName

    /************************************************************************************
     * Print this table.
     */
    public void print ()
    {
        out.println ("\n Table " + name);
        out.print ("|-");
        for (int i = 0; i < attribute.length; i++) out.print ("---------------");
        out.println ("-|");
        out.print ("| ");
        for (String a : attribute) out.printf ("%15s", a);
        out.println (" |");
        out.print ("|-");
        for (int i = 0; i < attribute.length; i++) out.print ("---------------");
        out.println ("-|");
        for (Comparable [] tup : tuples) {
            out.print ("| ");
            for (Comparable attr : tup) out.printf ("%15s", attr);
            out.println (" |");
        } // for
        out.print ("|-");
        for (int i = 0; i < attribute.length; i++) out.print ("---------------");
        out.println ("-|");
    } // print

    /************************************************************************************
     * Print this table's index (Map).
     */
    public void printIndex ()
    {
        out.println ("\n Index for " + name);
        out.println ("-------------------");
        for (Map.Entry <KeyType, Comparable []> e : index.entrySet ()) {
            out.println (e.getKey () + " -> " + Arrays.toString (e.getValue ()));
        } // for
        out.println ("-------------------");
    } // printIndex

    /************************************************************************************
     * Load the table with the given name into memory. 
     *
     * @param name  the name of the table to load
     */
    public static Table load (String name)
    {
        Table tab = null;
        try {
            ObjectInputStream ois = new ObjectInputStream (new FileInputStream (DIR + name + EXT));
            tab = (Table) ois.readObject ();
            ois.close ();
        } catch (IOException ex) {
            out.println ("load: IO Exception");
            ex.printStackTrace ();
        } catch (ClassNotFoundException ex) {
            out.println ("load: Class Not Found Exception");
            ex.printStackTrace ();
        } // try
        return tab;
    } // load

    /************************************************************************************
     * Save this table in a file.
     */
    public void save ()
    {
        try {
            ObjectOutputStream oos = new ObjectOutputStream (new FileOutputStream (DIR + name + EXT));
            oos.writeObject (this);
            oos.close ();
        } catch (IOException ex) {
            out.println ("save: IO Exception");
            ex.printStackTrace ();
        } // try
    } // save

    //----------------------------------------------------------------------------------
    // Private Methods
    //----------------------------------------------------------------------------------

    /************************************************************************************
     * Determine whether the two tables (this and table2) are compatible, i.e., have
     * the same number of attributes each with the same corresponding domain.
     *
     * @param table2  the rhs table
     * @return  whether the two tables are compatible
     */
    private boolean compatible (Table table2)
    {
        if (domain.length != table2.domain.length) {
            out.println ("compatible ERROR: table have different arity");
            return false;
        } // if
        for (int j = 0; j < domain.length; j++) {
            if (domain [j] != table2.domain [j]) {
                out.println ("compatible ERROR: tables disagree on domain " + j);
                return false;
            } // if
        } // for
        return true;
    } // compatible

    /************************************************************************************
     * Match the column and attribute names to determine the domains.
     *
     * @param column  the array of column names
     * @return  an array of column index positions
     */
    private int [] match (String [] column)
    {
        int [] colPos = new int [column.length];

        for (int j = 0; j < column.length; j++) {
            boolean matched = false;
            for (int k = 0; k < attribute.length; k++) {
                if (column [j].equals (attribute [k])) {
                    matched = true;
                    colPos [j] = k;
                } // for
            } // for
            if ( ! matched) {
                out.println ("match: domain not found for " + column [j]);
            } // if
        } // for

        return colPos;
    } // match

    /************************************************************************************
     * Extract the attributes specified by the column array from tuple t.
     *
     * @param t       the tuple to extract from
     * @param column  the array of column names
     * @return  a smaller tuple extracted from tuple t 
     */
    private Comparable [] extract (Comparable [] t, String [] column)
    {
        Comparable [] tup = new Comparable [column.length];
        int [] colPos = match (column);
        for (int j = 0; j < column.length; j++) tup [j] = t [colPos [j]];
        return tup;
    } // extract

    /************************************************************************************
     * Check the size of the tuple (number of elements in list) as well as the type of
     * each value to ensure it is from the right domain. 
     *
     * @param t  the tuple as a list of attribute values
     * @return  whether the tuple has the right size and values that comply
     *          with the given domains
     * TTTYYYPPPEEE CCCCCHHHHHEEEEECCCCCKKKKK
     */
    private boolean typeCheck (Comparable [] t)
    { 
        //  T O   B E   I M P L E M E N T E D 

        return true;
    } // typeCheck

    /************************************************************************************
     * Find the classes in the "java.lang" package with given names.
     *
     * @param className  the array of class name (e.g., {"Integer", "String"})
     * @return  an array of Java classes
     */
    private static Class [] findClass (String [] className)
    {
        Class [] classArray = new Class [className.length];

        for (int i = 0; i < className.length; i++) {
            try {
                classArray [i] = Class.forName ("java.lang." + className [i]);
            } catch (ClassNotFoundException ex) {
                out.println ("findClass: " + ex);
            } // try
        } // for

        return classArray;
    } // findClass

    /************************************************************************************
     * Extract the corresponding domains.
     *
     * @param colPos the column positions to extract.
     * @param group  where to extract from
     * @return  the extracted domains
     */
    private Class [] extractDom (int [] colPos, Class [] group)
    {
        Class [] obj = new Class [colPos.length];

        for (int j = 0; j < colPos.length; j++) {
            obj [j] = group [colPos [j]];
        } // for

        return obj;
    } // extractDom

} // Table class
