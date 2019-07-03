package multilines;


import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;

public class xmlReader {


        public static void main(String[] args) throws org.xml.sax.SAXException {
            final DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();

            try {
                final DocumentBuilder builder = factory.newDocumentBuilder();
                final Document document= builder.parse(new File(args[0]));
                final Element racine = document.getDocumentElement();
                final NodeList racineNoeuds = racine.getChildNodes();

                final int nbRacineNoeuds = racineNoeuds.getLength();

                for (int i = 0; i<nbRacineNoeuds; i++) {
                    //System.out.println(racineNoeuds.item(i).getNodeName());
                }

                for (int i = 0; i<nbRacineNoeuds; i++) {
                    if(racineNoeuds.item(i).getNodeType() == Node.ELEMENT_NODE) {
                        final Node personne = racineNoeuds.item(i);
                        //System.out.println(personne.getNodeName());
                        //System.out.println(personne.getFirstChild().);
                    }


                }


            }
            catch (final ParserConfigurationException e) {
                e.printStackTrace();
            } catch (final IOException e) {
                e.printStackTrace();
            }



        }
}
