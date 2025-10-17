package serveur_chat;

import java.io.*;
import java.net.*;
import java.util.*;

/**
 * EXERCICE 2 : Serveur de Chat TCP Multithread
 * 
 * Objectif :
 * Développer un serveur de chat capable de gérer plusieurs clients en parallèle.
 * Le serveur diffuse les messages à tous (broadcast) ou à un client ciblé (direct).
 * 
 * Protocole :
 * - Message sans ':' → broadcast à tous les autres clients
 * - Message 'N:texte' → envoi direct au client numéro N
 * 
 * @author ENSIASD - TP1 Systèmes d'Information Distribués
 */
public class ServeurChat extends Thread {
    
    private int nbClient = 0;                               // Compteur de clients
    private static List<Conversation> clients = new ArrayList<>();  // Liste des clients connectés
    
    /**
     * Méthode principale du thread serveur
     */
    @Override
    public void run() {
        try {
            // ÉTAPE 1 : Créer le ServerSocket sur le port 1234
            ServerSocket serverSocket = new ServerSocket(1234);
            System.out.println("╔════════════════════════════════════════════════╗");
            System.out.println("║    SERVEUR DE CHAT DÉMARRÉ SUR PORT 1234       ║");
            System.out.println("╚════════════════════════════════════════════════╝");
            System.out.println("[SERVEUR] En attente de connexions...\n");
            
            // ÉTAPE 3 : Boucle infinie pour accepter les connexions
            while (true) {
                // Attendre qu'un client se connecte
                Socket socket = serverSocket.accept();
                
                // Incrémenter le compteur de clients
                nbClient++;
                
                System.out.println("→ Nouveau client #" + nbClient + " connecté depuis : " + 
                                   socket.getRemoteSocketAddress());
                
                // ÉTAPE 3 : Créer un thread Conversation pour ce client
                Conversation conv = new Conversation(socket, nbClient);
                
                // ÉTAPE 3 : Ajouter à la liste des clients (synchronisé)
                synchronized (clients) {
                    clients.add(conv);
                }
                
                // ÉTAPE 3 : Démarrer le thread
                conv.start();
            }
            
        } catch (IOException e) {
            System.err.println("❌ Erreur serveur : " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * CLASSE INTERNE : Conversation
     * Gère la communication avec UN client dans un thread séparé
     */
    static class Conversation extends Thread {
        private Socket socketClient;
        private int numero;
        private BufferedReader br;
        private PrintWriter pw;
        private String clientIP;
        
        /**
         * Constructeur
         * @param socket Socket de connexion avec le client
         * @param num Numéro du client
         */
        public Conversation(Socket socket, int num) {
            this.socketClient = socket;
            this.numero = num;
        }
        
        @Override
        public void run() {
            try {
                // ÉTAPE 4 : Préparer les flux d'entrée/sortie
                br = new BufferedReader(
                    new InputStreamReader(socketClient.getInputStream())
                );
                pw = new PrintWriter(socketClient.getOutputStream(), true);
                
                // ÉTAPE 6 : Récupérer l'adresse IP du client
                clientIP = socketClient.getRemoteSocketAddress().toString();
                
                // ÉTAPE 7 : Logger la connexion
                System.out.println("  [Client #" + numero + "] " + clientIP + " - Connecté");
                
                // ÉTAPE 7 : Envoyer le message de bienvenue + rappel du protocole
                pw.println("╔═══════════════════════════════════════════╗");
                pw.println("║      BIENVENUE SUR LE SERVEUR CHAT !     ║");
                pw.println("╚═══════════════════════════════════════════╝");
                pw.println("");
                pw.println("✓ Vous êtes le client n°" + numero);
                pw.println("");
                pw.println("PROTOCOLE DE COMMUNICATION :");
                pw.println("  • Message simple → broadcast à tous");
                pw.println("  • N:message → envoi direct au client N");
                pw.println("  Exemple : '2:Bonjour' envoie à client #2");
                pw.println("───────────────────────────────────────────");
                pw.println("");
                
                // ÉTAPE 8 : Boucle de lecture des messages
                String line;
                while ((line = br.readLine()) != null) {
                    
                    // ÉTAPE 10 : trim() et ignorer les lignes vides
                    line = line.trim();
                    if (line.isEmpty()) {
                        continue;
                    }
                    
                    // Logger le message reçu
                    System.out.println("  [Client #" + numero + "] : " + line);
                    
                    // Vérifier si c'est un message direct (contient ':')
                    if (line.contains(":")) {
                        // CAS A : Message direct
                        
                        // Découper en 2 parties maximum (split(':', 2))
                        String[] parts = line.split(":", 2);
                        
                        // ÉTAPE : Valider que la partie gauche est un entier positif
                        if (parts[0].matches("\\d+")) {
                            try {
                                int numCible = Integer.parseInt(parts[0]);
                                String message = parts.length > 1 ? parts[1].trim() : "";
                                
                                // ÉTAPE : Appeler broadcastMessage avec numCible
                                broadcastMessage("[Client #" + numero + " → vous] " + message, 
                                               socketClient, numCible);
                                
                                // Confirmer l'envoi à l'expéditeur
                                pw.println("✓ Message envoyé au client #" + numCible);
                                
                                // Log serveur
                                System.out.println("    → Message direct de #" + numero + 
                                                 " vers #" + numCible);
                                
                            } catch (NumberFormatException e) {
                                pw.println("⚠️  Erreur : Numéro de client invalide");
                            }
                        } else {
                            // Pas un nombre valide, traiter comme broadcast
                            // CAS B : Broadcast
                            broadcastMessage("[Client #" + numero + "] " + line, 
                                           socketClient, -1);
                            System.out.println("    → Broadcast à tous");
                        }
                        
                    } else {
                        // CAS B : Broadcast (ligne sans ':')
                        // ÉTAPE : Appeler broadcastMessage avec -1 (tous)
                        broadcastMessage("[Client #" + numero + "] " + line, 
                                       socketClient, -1);
                        System.out.println("    → Broadcast à tous");
                    }
                }
                
                // ÉTAPE 9 : Si line == null, le client s'est déconnecté
                System.out.println("  [Client #" + numero + "] Déconnecté");
                
            } catch (IOException e) {
                System.err.println("❌ Erreur avec client #" + numero + " : " + e.getMessage());
                
            } finally {
                // ÉTAPE 15-16-17 : Nettoyage et fermeture
                cleanup();
            }
        }
        
        /**
         * ÉTAPE 11-12-13-14 : Diffuser un message aux clients connectés
         * 
         * @param message Le message à envoyer
         * @param expediteur Le socket de l'expéditeur (ne pas lui renvoyer)
         * @param numClient Le numéro du client cible (-1 pour broadcast à tous)
         */
        private void broadcastMessage(String message, Socket expediteur, int numClient) {
            // ÉTAPE 11 : Parcourir la liste des clients
            synchronized (clients) {
                for (Conversation client : clients) {
                    
                    // ÉTAPE 12 : Ne pas renvoyer au client émetteur
                    if (client.socketClient.equals(expediteur)) {
                        continue;
                    }
                    
                    // ÉTAPE 13 : Si numClient == -1 → tous, sinon → client spécifique
                    if (numClient == -1 || client.numero == numClient) {
                        try {
                            // ÉTAPE 14 : Utiliser PrintWriter en auto-flush
                            client.pw.println(message);
                        } catch (Exception e) {
                            System.err.println("⚠️  Erreur lors de l'envoi à #" + 
                                             client.numero + " : " + e.getMessage());
                        }
                    }
                }
            }
        }
        
        /**
         * ÉTAPE 15-16-17 : Nettoyage des ressources
         */
        private void cleanup() {
            // ÉTAPE 15 : Fermer les flux et le socket
            try {
                if (br != null) br.close();
                if (pw != null) pw.close();
                if (socketClient != null) socketClient.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            
            // ÉTAPE 16 : Retirer de la liste des clients
            synchronized (clients) {
                clients.remove(this);
            }
            
            // ÉTAPE 17 : Logger la déconnexion
            System.out.println("  [Client #" + numero + "] Nettoyage terminé - " + 
                             clients.size() + " client(s) restant(s)");
        }
    }
    
    /**
     * POINT D'ENTRÉE DU PROGRAMME
     */
    public static void main(String[] args) {
        System.out.println("\n╔════════════════════════════════════════════════╗");
        System.out.println("║         SERVEUR DE CHAT MULTITHREAD            ║");
        System.out.println("║              TP1 - ENSIASD                     ║");
        System.out.println("║         Systèmes d'Information Distribués      ║");
        System.out.println("╚════════════════════════════════════════════════╝\n");
        
        // Démarrer le serveur dans un thread
        new ServeurChat().start();
    }
}