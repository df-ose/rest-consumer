package com.respuesta.Usercontroller;

import java.io.IOException;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.respuesta.Consumer.Consumidor;

@RestController
public class Usercontroller {

	
	
	private String datas ;
	
	//private consumer respuesta = new consumer();
	
	@GetMapping("/respuesta")
    
	public String greeting(@RequestParam(value = "name", defaultValue = "sin respuesta") String name) throws JsonProcessingException, IOException 
	{
		
		Consumidor respuesta = new Consumidor();
		
		datas=respuesta.ejecutar(); 
		
               return datas;
    }	
	
	
	
	
}
