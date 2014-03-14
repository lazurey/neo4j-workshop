package com.thoughtworks;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.IndexManager;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileNotFoundException;
import java.io.IOException;

public class GenerateTopology {

    private final static String DB_PATH = "/Users/twer/Documents/projects/thoughtworks/neo4j-workshop/graph";

    private static enum Relationships implements RelationshipType {
        SPONSOR, BUDDY, EMPLOYEE
    }

    public static void main(String[] args) {
        GraphDatabaseService graph = new GraphDatabaseFactory().newEmbeddedDatabase(DB_PATH);
        registerShutdownHook(graph);
        Transaction tx = graph.beginTx();

        Label worker = DynamicLabel.label("Worker");
        Label company = DynamicLabel.label("Company");

        String csvFile = "/Users/twer/Documents/projects/thoughtworks/sponsor-sponsee.csv";
        BufferedReader br = null;
        String line;
        String csvSplitBy = "\",\"";

        try {
            IndexManager index = graph.index();
            Index<Node> workers = index.forNodes( "workers" );

            Node root = graph.createNode(company);
            root.setProperty("name", "ThoughtWorks");

            br = new BufferedReader(new FileReader(csvFile));
            while ((line = br.readLine()) != null) {
                String[] pair = line.split(csvSplitBy);
                String sponsor = pair[0].substring(1);
                String sponsee = pair[1].substring(0, pair[1].length() - 1);

                IndexHits<Node> sponsorHits = workers.get("name", sponsor);
                Node sponsorNode, sponseeNode;
                if (sponsorHits.size() == 1) {
                    sponsorNode = sponsorHits.getSingle();
                } else {
                    sponsorNode = graph.createNode(worker);
                    sponsorNode.setProperty("name", sponsor);
                    workers.add(sponsorNode, "name", sponsorNode.getProperty("name"));
                    root.createRelationshipTo(sponsorNode, Relationships.EMPLOYEE);
                }

                IndexHits<Node> sponseeHits = workers.get("name", sponsee);
                if (sponseeHits.size() == 1) {
                    sponseeNode = sponseeHits.getSingle();
                    if (sponseeNode.hasRelationship(Direction.INCOMING, Relationships.EMPLOYEE)) {
                        sponseeNode.getSingleRelationship(Relationships.EMPLOYEE, Direction.INCOMING).delete();
                    }
                } else {
                    sponseeNode = graph.createNode(worker);
                    sponseeNode.setProperty("name", sponsee);
                    workers.add(sponseeNode, "name", sponseeNode.getProperty("name"));
                }
                sponsorNode.createRelationshipTo(sponseeNode, Relationships.SPONSOR);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            tx.success();
            tx.close();
        }
        graph.shutdown();

    }

    private static void registerShutdownHook(final GraphDatabaseService graph) {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                graph.shutdown();
            }
        });
    }

}
