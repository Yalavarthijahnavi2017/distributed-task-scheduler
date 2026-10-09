package com.scheduler;

/**
 * Entry point.
 *
 * <pre>
 *   java com.scheduler.SchedulerApp produce   # submit demo tasks
 *   java com.scheduler.SchedulerApp worker    # start a worker
 * </pre>
 */
public class SchedulerApp {

    private static final String BOOTSTRAP = "localhost:9092";

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.out.println("Usage: SchedulerApp <produce|worker>");
            return;
        }
        switch (args[0]) {
            case "produce":
                try (TaskProducer producer = new TaskProducer(BOOTSTRAP)) {
                    producer.submit("email", "welcome-email:user@example.com");
                    producer.submit("report", "weekly-sales-report");
                    producer.submit("email", "reminder:user2@example.com");
                    System.out.println("Submitted 3 demo tasks.");
                }
                break;
            case "worker":
                TaskWorker worker = new TaskWorker(BOOTSTRAP, "scheduler-workers");
                Runtime.getRuntime().addShutdownHook(new Thread(worker::shutdown));
                try (worker) {
                    worker.run();
                }
                break;
            default:
                System.out.println("Unknown mode: " + args[0]);
        }
    }
}
