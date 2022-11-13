package com.udacity.webcrawler;

import com.udacity.webcrawler.parser.PageParser;
import com.udacity.webcrawler.parser.PageParserFactory;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;


/**
 * This is the Task class that perform the parsing with fork join pool.
 */

public class CrawlTask extends RecursiveAction {
    private String url;
    private Instant deadline;
    private int maxDepth;
    private Map<String, Integer> counts;
    private Set<String> visitedUrls;
    private PageParserFactory parserFactory;
    private List<Pattern> ignoredUrls;
    private Clock clock;


    /**
     * This is the private constructor that will be used by the Builder class.
     *
     * @param url
     * @param deadline
     * @param maxDepth
     * @param counts
     * @param visitedUrls
     * @param parserFactory
     * @param ignoredUrls
     * @param clock
     */
    private CrawlTask(String url, Instant deadline, int maxDepth, Map<String, Integer> counts, Set<String> visitedUrls,
                      PageParserFactory parserFactory, List<Pattern> ignoredUrls, Clock clock) {
        this.url = url;
        this.deadline = deadline;
        this.maxDepth = maxDepth;
        this.counts = counts;
        this.visitedUrls = visitedUrls;
        this.parserFactory = parserFactory;
        this.ignoredUrls = ignoredUrls;
        this.clock = clock;
    }

    /**
     * The Builder class for CrawlTask
     */
    public static final class Builder {
        private String url;
        private Instant deadline;
        private int maxDepth;
        private Map<String, Integer> counts;
        private Set<String> visitedUrls;
        private PageParserFactory parserFactory;
        private List<Pattern> ignoredUrls;
        private Clock clock;

        public Builder setUrl(String url) {
            this.url = Objects.requireNonNull(url);
            return this;
        }

        public Builder setDeadline(Instant deadline) {
            this.deadline = Objects.requireNonNull(deadline);
            return this;
        }

        public Builder setMaxDepth(int maxDepth) {
            this.maxDepth = maxDepth;
            return this;
        }

        public Builder setCounts(Map<String, Integer> counts) {
            this.counts = Objects.requireNonNull(counts);
            return this;
        }

        public Builder setVisitedUrls(Set<String> visitedUrls) {
            this.visitedUrls = Objects.requireNonNull(visitedUrls);
            return this;
        }

        public Builder setParserFactory(PageParserFactory parserFactory) {
            this.parserFactory = parserFactory;
            return this;
        }

        public Builder setIgnoredUrls(List<Pattern> ignoredUrls) {
            this.ignoredUrls = ignoredUrls;
            return this;
        }

        public Builder setClock(Clock clock) {
            this.clock = clock;
            return this;
        }

        public CrawlTask build() {
            return new CrawlTask(url, deadline, maxDepth, counts, visitedUrls, parserFactory, ignoredUrls, clock);
        }
    }

    @Override
    protected void compute() {
        if (maxDepth == 0 || clock.instant().isAfter(deadline)) {
            return;
        }
        for (Pattern pattern : ignoredUrls) {
            if (pattern.matcher(url).matches()) {
                return;
            }
        }
        if (visitedUrls.contains(url)) {
            return;
        }
        visitedUrls.add(url);
        PageParser.Result result = parserFactory.get(url).parse();
        final Lock lock = new ReentrantLock();
        for (Map.Entry<String, Integer> e : result.getWordCounts().entrySet()) {
            lock.lock();
            if (counts.containsKey(e.getKey())) {
                counts.put(e.getKey(), e.getValue() + counts.get(e.getKey()));
            } else {
                counts.put(e.getKey(), e.getValue());
            }
            lock.unlock();
        }
        Builder builder = new Builder().setDeadline(deadline)
                .setMaxDepth(maxDepth - 1)
                .setCounts(counts)
                .setVisitedUrls(visitedUrls)
                .setParserFactory(parserFactory)
                .setIgnoredUrls(ignoredUrls)
                .setClock(clock);
        List<CrawlTask> subtasks = result.getLinks().stream().map(link ->
                builder.setUrl(link).build()).toList();
        invokeAll(subtasks);    // Recursive call.
    }
}
