package com.its.springtipsdebuggingreactor;

import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import reactor.blockhound.BlockHound;
import reactor.blockhound.integration.BlockHoundIntegration;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.tools.agent.ReactorDebugAgent;

import java.time.Duration;
import java.util.function.Consumer;
import java.util.function.Function;

@SpringBootApplication
@Log4j2
public class SpringTipsDebuggingReactorApplication {

	public static void main(String[] args) {
		ReactorDebugAgent.init();
		ReactorDebugAgent.processExistingClasses();

		/** one can register specific integration based on the need
		 * eg. Logging, Reactor, Rxjava
		 * */

		/*BlockHound.install(new BlockHoundIntegration() {
			@Override
			public void applyTo(BlockHound.Builder builder) {

			}
		});*/

		BlockHound.install();
		// Does runtime instrumentation which  is not desirable for  production. So use above 2 steps to avoid this
		/*Hooks.onOperatorDebug();*/

		SpringApplication.run(SpringTipsDebuggingReactorApplication.class, args);
	}

	@EventListener(ApplicationReadyEvent.class)
	public void go() throws Exception{
		log.info("Entering SpringTipsDebuggingReactorApplication : go");
		// generate stream of data
		/*Flux <String> letters = Flux
									.just("A", "B", "C", "D", "E", "F")
									.checkpoint("Capital letters", false)
									.map(l -> l.toLowerCase())
									.checkpoint("lower case letters", false)
									.flatMap( letter -> {
										if (letter.equalsIgnoreCase("F"))
											return Mono.error(new IllegalLetterException());
										else
											return Mono.just(letter);
									})
									*//*.map(l -> {
										if (l.equalsIgnoreCase("f"))
											throw new IllegalLetterException();
										return l;
									})*//*
									.checkpoint("I want en error to show this message", false)
									.subscribeOn(Schedulers.elastic(), true)
									.log()
									.delayElements(Duration.ofSeconds(1))
									.doOnError(this :: error);*/
		/** Below code is copy of above flow just to see how blockhound works and helps identifying
		 *  blocking calls
		 * */
		Scheduler cantBlock = Schedulers.newParallel("p5", 5);
		Scheduler canBlock = Schedulers.elastic();

		Flux <String> letters = Flux
				.just("A", "B", "C", "D", "E", "F")
				.checkpoint("Capital letters", false)
				.map(l -> l.toLowerCase())
				.checkpoint("lower case letters", false)
				.flatMap( Mono :: just)
				.doOnNext(x -> block())
				.checkpoint("I want en error to show this message", false)
				.subscribeOn(cantBlock, true)
				.log()
				.delayElements(Duration.ofSeconds(1))
				.doOnError(this :: error);

		processPublisher(letters);
		//letters.subscribe(this :: info, this :: error);

		/**
		 * Cold consumer i.e. it gets data whenever it is available
		 * */
		//Flux<String> share = letters.share();
		//Consumer<String> consumer = letter -> log.info("New Letter : " + letter);

		//letters.subscribe(consumer);
		/**
		 * We have an overloaded version ob subscribe i.e. which allows to handle error
		 * So commenting above and adding overloaded method as below
		 * */
		//letters.subscribe(consumer, log::error);
		//Thread.sleep(2 * 1000);
		/**
		 * Share wont print entire pipeline. It would rather print the in flight stream of data
		 * */
		//share.subscribe(consumer);
	}

	static class IllegalLetterException extends RuntimeException {
		public IllegalLetterException() {
			super("Cant be F! No Fs' please");
		}
	}

	void error(Throwable t) {
		log.error("Thread Info : " + Thread.currentThread().getName());
		log.error("Oh Noes !");
		log.error(t);
	}

	void info(String letter) {
		log.info("Thread Info : " + Thread.currentThread().getName());
		log.info("The current value is : " + letter);
	}

	void processPublisher (Flux<String> letters) {
		letters.subscribe(this :: info);
	}

	@SneakyThrows
	void block() {
		Thread.sleep(1000);
	}
}
