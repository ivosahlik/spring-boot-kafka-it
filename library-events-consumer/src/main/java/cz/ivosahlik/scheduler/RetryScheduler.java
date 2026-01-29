package cz.ivosahlik.scheduler;

import cz.ivosahlik.config.LibraryEventsConsumerConfig;
import cz.ivosahlik.entity.FailureRecord;
import cz.ivosahlik.jpa.FailureRecordRepository;
import cz.ivosahlik.service.LibraryEventsService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;

@RequiredArgsConstructor
@Slf4j
public class RetryScheduler {

    final LibraryEventsService libraryEventsService;
    final FailureRecordRepository failureRecordRepository;

    @Scheduled(fixedRate = 10_000 )
    public void retryFailedRecords(){

        log.info("Retrying Failed Records Started!");
        var status = LibraryEventsConsumerConfig.RETRY;
        failureRecordRepository.findAllByStatus(status)
                .forEach(failureRecord -> {
                    try {
                        var consumerRecord = buildConsumerRecord(failureRecord);
                        libraryEventsService.processLibraryEvent(consumerRecord);
                        failureRecord.setStatus(LibraryEventsConsumerConfig.SUCCESS);
                    } catch (Exception e){
                        log.error("Exception in retryFailedRecords : ", e);
                    }
                });
    }

    private ConsumerRecord<Integer, String> buildConsumerRecord(FailureRecord failureRecord) {

        return new ConsumerRecord<>(failureRecord.getTopic(),
                failureRecord.getPartition(), failureRecord.getOffset_value(), failureRecord.getKey_value(),
                failureRecord.getErrorRecord());

    }
}
