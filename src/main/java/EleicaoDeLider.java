import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import static org.apache.zookeeper.Watcher.Event.EventType.None;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class EleicaoDeLider {

    private static final String HOST = "localhost";
    private static final String PORTA = "2181";
    private static final int TIMEOUT = 5000;
    private static final String NAMESPACE_ELEICAO = "/eleicao";
    private static final String ZNODE_TESTE_WATCH = "/teste_watcher";
    private String nomeDoZNodeDesseProcesso;
    private ZooKeeper zooKeeper;

    public void realizarCandidatura() throws InterruptedException, KeeperException {
        String prefixo = String.format("%s/cand_", NAMESPACE_ELEICAO);
        String pathInteiro = zooKeeper.create(prefixo, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        this.nomeDoZNodeDesseProcesso = pathInteiro.replace(String.format("%s/", NAMESPACE_ELEICAO), "");
        System.out.println(this.nomeDoZNodeDesseProcesso);
    }


    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {

        System.out.println("Método main: " + Thread.currentThread().getName());
        EleicaoDeLider eleicaoDeLider = new EleicaoDeLider();
        eleicaoDeLider.conectar();
        eleicaoDeLider.realizarCandidatura();
        eleicaoDeLider.eleicaoEReeleicao();
//        eleicaoDeLider.elegerOLider();
        //eleicaoDeLider.registrarWatcher();
        eleicaoDeLider.executar();
        eleicaoDeLider.fechar();
    }

    //expressão lambda
    private Watcher reeleicaoWatcher = (event) -> {
        try{
            switch (event.getType()){
                case NodeDeleted:
                //lider pode ser se tornado inoperante.
                //Nova eleição
                    eleicaoEReeleicao();
                break;
            }
        }
        catch (Exception e){
            e.printStackTrace();
        }
    };



    public void fechar() throws InterruptedException {
        zooKeeper.close();
    }

    public void executar() throws InterruptedException {
        synchronized (zooKeeper) {
            zooKeeper.wait();
        }
    }

    public void eleicaoEReeleicao() throws InterruptedException, KeeperException{
        Stat statPredecessor = null;
        String nomePredecessor = "";
        do{
            List <String> candidatos = zooKeeper.getChildren(NAMESPACE_ELEICAO, false);
            Collections.sort(candidatos);
            String oMenor = candidatos.get(0);
            if (oMenor.equals(nomeDoZNodeDesseProcesso)){
                System.out.printf ("Me chamo %s e sou o líder.\n", nomeDoZNodeDesseProcesso);
                return;
            }
            System.out.printf ("Me chamo %s e não sou o líder. O líder é o %s\n", nomeDoZNodeDesseProcesso, oMenor);
            int indicePredecessor = Collections.binarySearch(candidatos, nomeDoZNodeDesseProcesso) - 1;
            nomePredecessor = candidatos.get(indicePredecessor);
            statPredecessor = zooKeeper.exists(
                    String.format("%s/%s", NAMESPACE_ELEICAO, nomePredecessor),
                    reeleicaoWatcher
            );
        }while (statPredecessor == null);
        System.out.printf ("Estou observando o %s\n", nomePredecessor);
    }




    public void elegerOLider() throws InterruptedException, KeeperException {
        //obter a lista de filhos do ZNode /eleicao
        //usar o zooKeeper
        List<String> filhos = zooKeeper.getChildren(NAMESPACE_ELEICAO, false);
        //ordenar a lista de filhos
        Collections.sort(filhos);
        //Verificar o primeiro da lista é igual ao atual nomeDoZNodeDesseProcesso
        //se for, declarar-se lider. Caso contrário, dizer quem é o lider
        String lider = filhos.get(0);
        if (lider.equals(nomeDoZNodeDesseProcesso)) {
            System.out.printf("Me chamo %s e sou o líder.\n", nomeDoZNodeDesseProcesso);
        } else {
            System.out.printf("Não sou o líder. O líder é o %s.\n", lider);
        }


    }

    public void conectar() throws IOException {
        zooKeeper = new ZooKeeper(
                String.format("%s:%s", HOST, PORTA),
                TIMEOUT,
                evento -> {
                    if (evento.getType() == None) {
                        if (evento.getState() == Watcher.Event.KeeperState.SyncConnected) {
                            System.out.println("Tratando evento: " + Thread.currentThread().getName());
                            System.out.println("Conectou");
                        } else if (evento.getState() == Watcher.Event.KeeperState.Disconnected) {
                            synchronized (zooKeeper) {
                                System.out.println("Desconectou");
                                System.out.println("Estamos na thread: " + Thread.currentThread().getName());
                                zooKeeper.notify();
                            }
                        }
                    }
                }
        );
    }
}
//    private class TesteWatcher implements Watcher {
//        @Override
//        public void process(WatchedEvent event) {
//            System.out.println(event);
//            switch (event.getType()) {
//                case NodeCreated:
//                    System.out.println("ZNode criado");
//                    break;
//                case NodeDeleted:
//                    System.out.println("ZNode removido");
//                    break;
//                case NodeDataChanged:
//                    System.out.println("Dados do ZNode alterados");
//                    break;
//                case NodeChildrenChanged:
//                    System.out.println("Evento envolvendo os filhos");
//            }
//            try {
//                registrarWatcher();
//            } catch (Exception e) {
//                System.out.println(e.getMessage());
//            }
//
//        }
//    }

//    public void registrarWatcher() throws InterruptedException, KeeperException {
//
//        TesteWatcher watcher = new TesteWatcher();
//        Stat stat = zooKeeper.exists(ZNODE_TESTE_WATCH, watcher);
//        //znode existe
//        if (stat != null) {
//            byte[] bytes = zooKeeper.getData(ZNODE_TESTE_WATCH, watcher, stat);
//            String dados = bytes != null ? new String(bytes) : "";
//            System.out.printf("Dados: %s\n", dados);
//            List<String> filhos = zooKeeper.getChildren(ZNODE_TESTE_WATCH, watcher);
//            System.out.println("Filhos: " + filhos);
//        }
//    }
//    private class TesteWatcher implements Watcher{
//
//        @Override
//        public void process(WatchedEvent event) {
//            System.out.println(event);
//            try{
//                switch (event.getType()){
//                    case NodeCreated:
//                        System.out.println ("ZNode criado");
//                        break;
//                    case NodeDeleted:
//                        System.out.println ("ZNode removido");
//                        break;
//                    case NodeDataChanged:
//                        System.out.println ("Dados do ZNode alterados");
//                        Stat stat = zooKeeper.exists(ZNODE_TESTE_WATCH, false);
//                        byte [] bytes = zooKeeper.getData(ZNODE_TESTE_WATCH, false, stat);
//                        String dados = bytes != null ? new String (bytes) : "";
//                        System.out.println ("Dados: " + dados);
//                        break;
//                        //não vai acontecer mais
//                    case NodeChildrenChanged:
//                        System.out.println ("Não deveria acontecer...filhos alterados.");
//                        break;
//                }
//            }
//            catch (Exception e){
//                e.printStackTrace();
//            }
//        }
//    }
//
//    public void registrarWatcher (){
//        TesteWatcher watcher = new TesteWatcher();
//        try{
//            zooKeeper.addWatch(ZNODE_TESTE_WATCH, watcher, AddWatchMode.PERSISTENT_RECURSIVE);
//        }
//        catch (Exception e){
//            e.printStackTrace();
//        }
//    }
