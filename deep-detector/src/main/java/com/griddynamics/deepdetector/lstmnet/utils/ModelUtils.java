package com.griddynamics.deepdetector.lstmnet.utils;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.deeplearning4j.nn.api.Layer;
import org.deeplearning4j.nn.conf.MultiLayerConfiguration;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * TODO
 * Project utility class to save and load models and parameters.
 */
public class ModelUtils {

    private static final Logger log = LoggerFactory.getLogger(ModelUtils.class);


    private ModelUtils() {
    }

    public static void saveModelAndParameters(MultiLayerNetwork net, String basePath) throws IOException {
        String confPath = FilenameUtils.concat(basePath, net.toString() + "-conf.json");
        String paramPath = FilenameUtils.concat(basePath, net.toString() + ".bin");
        log.info("Saving model and parameters to {} and {} ...", confPath, paramPath);

        // save parameters
        try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(paramPath))) {
            Nd4j.write(net.params(), dos);
            // save model configuration
            FileUtils.write(new File(confPath), net.getLayerWiseConfigurations().toJson());
        }

    }

    /**
     * TODO
     *
     * @param basePath
     * @return
     * @throws IOException
     */
    public static MultiLayerNetwork loadModelAndParameters(String basePath) throws IOException {

        String confPath = lookupAnyFileByNameEnding(basePath, "conf.json").getAbsolutePath();
        String paramsPath = lookupAnyFileByNameEnding(basePath, ".bin").getAbsolutePath();
        return loadModelAndParameters(confPath, paramsPath);
    }

    //TODO
    public static File lookupAnyFileByNameEnding(String basedir, final String ending) throws IOException {
        File dirToLookInto = new File(basedir);
        if (dirToLookInto == null ||
                !dirToLookInto.exists() ||
                !dirToLookInto.isDirectory())
            throw new IOException("can't find spesified directoryto lookup in: " + dirToLookInto);

        File[] results = dirToLookInto.listFiles(
                new FilenameFilter() {
                    @Override
                    public boolean accept(File dir, String name) {
                        return name.endsWith(ending);
                    }
                });

        if (results != null && results.length > 0) {
            return results[0];
        } else {
            return null;
        }
    }

    /**
     * TODO
     *
     * @param confPath
     * @param paramPath
     * @return
     * @throws IOException
     */
    public static MultiLayerNetwork loadModelAndParameters(String confPath, String paramPath) throws IOException {
        log.info("Loading saved model and parameters...");
        MultiLayerNetwork savedNetwork = null;

        File confFile = new File(confPath);
        if (confFile != null && confFile.isFile() && confFile.exists()) {
            // load parameters
            MultiLayerConfiguration confFromJson = MultiLayerConfiguration.fromJson(FileUtils.readFileToString(confFile));
            try (DataInputStream dis = new DataInputStream(new FileInputStream(paramPath))) {
                INDArray newParams = Nd4j.read(dis);

                // load model configuration
                savedNetwork = new MultiLayerNetwork(confFromJson);
                savedNetwork.init();
                savedNetwork.setParams(newParams);
            }
        }
        return savedNetwork;
    }


    public static void saveLayerParameters(INDArray param, String paramPath) throws IOException {
        // save parameters for each layer
        log.info("Saving parameters to {} ...", paramPath);

        try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(paramPath))) {
            Nd4j.write(param, dos);
        }
    }

    public static Layer loadLayerParameters(Layer layer, String paramPath) throws IOException {
        // load parameters for each layer
        String name = layer.conf().getLayer().getLayerName();
        log.info("Loading saved parameters for layer {} ...", name);

        try (DataInputStream dis = new DataInputStream(new FileInputStream(paramPath))) {
            INDArray param = Nd4j.read(dis);
            layer.setParams(param);
        }

        return layer;
    }

    public static void saveParameters(MultiLayerNetwork model, int[] layerIds, Map<Integer, String> paramPaths) throws IOException {
        Layer layer;
        for (int layerId : layerIds) {
            layer = model.getLayer(layerId);
            if (!layer.paramTable().isEmpty()) {
                ModelUtils.saveLayerParameters(layer.params(), paramPaths.get(layerId));
            }
        }
    }

    public static void saveParameters(MultiLayerNetwork model,
                                      String[] layerIds,
                                      Map<String, String> paramPaths) throws IOException {
        Layer layer;
        for (String layerId : layerIds) {
            layer = model.getLayer(layerId);
            if (!layer.paramTable().isEmpty()) {
                ModelUtils.saveLayerParameters(layer.params(), paramPaths.get(layerId));
            }
        }
    }

    public static MultiLayerNetwork loadParameters(MultiLayerNetwork model,
                                                   int[] layerIds,
                                                   Map<Integer, String> paramPaths) throws IOException {
        Layer layer;
        for (int layerId : layerIds) {
            layer = model.getLayer(layerId);
            loadLayerParameters(layer, paramPaths.get(layerId));
        }
        return model;
    }

    public static MultiLayerNetwork loadParameters(MultiLayerNetwork model,
                                                   String[] layerIds,
                                                   Map<String, String> paramPaths) throws IOException {
        Layer layer;
        for (String layerId : layerIds) {
            layer = model.getLayer(layerId);
            loadLayerParameters(layer, paramPaths.get(layerId));
        }
        return model;
    }

    public static Map<Integer, String> getIdParamPaths(MultiLayerNetwork model, String basePath, int[] layerIds) {
        Map<Integer, String> paramPaths = new HashMap<>();
        for (int id : layerIds) {
            paramPaths.put(id, FilenameUtils.concat(basePath, id + ".bin"));
        }

        return paramPaths;
    }

    public static Map<String, String> getStringParamPaths(MultiLayerNetwork model, String basePath, String[] layerIds) {
        Map<String, String> paramPaths = new HashMap<>();

        for (String name : layerIds) {
            paramPaths.put(name, FilenameUtils.concat(basePath, name + ".bin"));
        }

        return paramPaths;
    }

    public static String defineOutputDir(String modelType) {
        String tmpDir = System.getProperty("java.io.tmpdir");
        String outputPath = File.separator + modelType + File.separator + "output";
        File dataDir = new File(tmpDir, outputPath);
        if (!dataDir.getParentFile().exists())
            dataDir.mkdirs();
        return dataDir.toString();

    }

}