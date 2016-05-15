package proxysf;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.web.SpringBootServletInitializer;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableAutoConfiguration
@ComponentScan

public class TomcatApplication extends SpringBootServletInitializer {
	 @Override
	    protected SpringApplicationBuilder configure(SpringApplicationBuilder application) {
	        //return application.sources(TomcatApplication.class);
		 	return application;
	    }

	    public static void main(String[] args) throws Exception {
	        SpringApplication.run(TomcatApplication.class, args);
	    }
    

}

