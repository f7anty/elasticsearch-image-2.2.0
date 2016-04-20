package org.elasticsearch.plugin.image;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.image.ImageMapper;
import org.elasticsearch.index.query.image.ImageQueryParser;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.threadpool.ThreadPool;

public class ImagePlugin extends Plugin {

	private final ImageMapper.TypeParser imageMapperTypeParser;
    @Override
    public String name() {
        return "image";
    }

    @Override
    public String description() {
        return "Elasticsearch Image Plugin";
    }
    
    public ImagePlugin(Settings settings){
    	 ThreadPool threadPool=new ThreadPool(settings);
    	 this.imageMapperTypeParser=new ImageMapper.TypeParser(threadPool); 
    }

    public void onModule(IndicesModule indicesModule){
    	indicesModule.registerQueryParser(ImageQueryParser.class);   //注册查询解析类
		indicesModule.registerMapper("image", imageMapperTypeParser);// 注册type为"image"的处理类
    }
}
