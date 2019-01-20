package com.example.demo.controller;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import javax.validation.Valid;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.gridfs.GridFsCriteria;
import org.springframework.data.mongodb.gridfs.GridFsTemplate;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.ZeroCopyHttpOutputMessage;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.RestController;

import com.example.demo.model.Tweet;
import com.example.demo.repository.TweetRepository;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.model.GridFSFile;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
public class TweetController {

    private final Logger logger = LogManager.getLogger(getClass());

    @Autowired
    private TweetRepository tweetRepository;

    @Autowired
    private GridFsTemplate gridFsTemplate;

    @Autowired
    private GridFSBucket gridFSBucket;

    @GetMapping("/tweets")
    public Flux<Tweet> getAllTweets() {
        return tweetRepository.findAll();
    }

    @PostMapping("/tweets")
    public Mono<Tweet> createTweets(@Valid @RequestBody Tweet tweet) {
        return tweetRepository.save(tweet);
    }

    @GetMapping("/tweets/{id}")
    public Mono<ResponseEntity<Tweet>> getTweetById(@PathVariable(value = "id") String tweetId) {
        return tweetRepository.findById(tweetId)
            .map(savedTweet -> ResponseEntity.ok(savedTweet))
            .defaultIfEmpty(ResponseEntity.notFound().build());
    }

    @PutMapping("/tweets/{id}")
    public Mono<ResponseEntity<Tweet>> updateTweet(@PathVariable(value = "id") String tweetId,
        @Valid @RequestBody Tweet tweet) {
        return tweetRepository.findById(tweetId)
            .flatMap(existingTweet -> {
                existingTweet.setText(tweet.getText());
                return tweetRepository.save(existingTweet);
            })
            .map(updatedTweet -> new ResponseEntity<>(updatedTweet, HttpStatus.OK))
            .defaultIfEmpty(new ResponseEntity<>(HttpStatus.NOT_FOUND));
    }

    @DeleteMapping("/tweets/{id}")
    public Mono<ResponseEntity<Void>> deleteTweet(@PathVariable(value = "id") String tweetId) {

        return tweetRepository.findById(tweetId)
            .flatMap(existingTweet -> tweetRepository.delete(existingTweet)
                .then(Mono.just(new ResponseEntity<Void>(HttpStatus.OK))))
            .defaultIfEmpty(new ResponseEntity<>(HttpStatus.NOT_FOUND));
    }

    // Tweets are Sent to the client as Server Sent Events
    @GetMapping(value = "/stream/tweets", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<Tweet> streamAllTweets() {
        return tweetRepository.findAll();
    }

    @PostMapping("/upload/{id}")
    public void upload(@RequestPart("file") FilePart filePart, @PathVariable(value = "id") String id)
        throws IOException {
        File file = File.createTempFile(id, "png");
        filePart.transferTo(file);
        try (InputStream is = new FileInputStream(file)) {
            gridFsTemplate.store(is, id);
            logger.info("store file success");
        } finally {
            file.delete();
        }
    }

    @GetMapping("/download/{id}")
    public Mono<Void> download(ServerHttpResponse response, @PathVariable(value = "id") String id)
        throws IllegalStateException, IOException {
        File file = new File("D:\\Temp\\t.png");
        ZeroCopyHttpOutputMessage zeroCopyResponse = (ZeroCopyHttpOutputMessage)response;
        response.getHeaders().set(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=parallel.png");
        response.getHeaders().setContentType(MediaType.IMAGE_PNG);
        GridFSFile gridFSFile = gridFsTemplate.findOne(Query.query(GridFsCriteria.whereFilename().is(id)));
        if (gridFSFile == null) {
            logger.error("store file {} not exists.", id);
            return Mono.empty();
        }
        try (InputStream is = gridFSBucket.openDownloadStream(gridFSFile.getObjectId());
            OutputStream os = new FileOutputStream(file)) {
            byte[] bytes = new byte[1024];
            int len;
            while ((len = is.read(bytes)) > 0) {
                os.write(bytes, 0, len);
            }
        }
        return zeroCopyResponse.writeWith(file, 0, file.length());
    }
}