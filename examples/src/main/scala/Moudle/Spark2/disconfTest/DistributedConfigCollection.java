package Moudle.Spark2.disconfTest;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Created by eason on 15/9/11.
 * esconfig，kafkaconfig，esmapping的配置管理集合
 */
@Service
public class DistributedConfigCollection implements java.io.Serializable {

    private static final Logger LOG = Logger.getLogger(DistributedConfigCollection.class);



    @Autowired
    private HBaseConfig hBaseConfig;



    public DistributedConfigCollection() {

    }



    public HBaseConfig getHBaseConfig() {
        return hBaseConfig;
    }




}
