package proxysf;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.web.SpringBootServletInitializer;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.WebApplicationInitializer;

@Configuration
@EnableAutoConfiguration
@ComponentScan

public class WLSApplication extends SpringBootServletInitializer implements WebApplicationInitializer {
	 
	    

	  public static void main(String[] args){
		 // DeployEnv.getInstance().setEnv("WLS");
		  SpringApplication.run(WLSApplication.class, args);
	  }

		
    

}

