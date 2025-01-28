import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Implement the two methods below. We expect this class to be stateless and thread safe.
 */
public class Census {
    /**
     * Number of cores in the current machine.
     */
    private static final int CORES = Runtime.getRuntime().availableProcessors();

    /**
     * Output format expected by our tests.
     */
    public static final String OUTPUT_FORMAT = "%d:%d=%d"; // Position:Age=Total

    /**
     * Factory for iterators.
     */
    private final Function<String, Census.AgeInputIterator> iteratorFactory;

    /**
     * Creates a new Census calculator.
     *
     * @param iteratorFactory factory for the iterators.
     */
    public Census(Function<String, Census.AgeInputIterator> iteratorFactory) {
        this.iteratorFactory = iteratorFactory;
    }

    /**
     * Given one region name, call {@link #iteratorFactory} to get an iterator for this region and return
     * the 3 most common ages in the format specified by {@link #OUTPUT_FORMAT}.
     */
    public String[] top3Ages(String region) {
        List<Map.Entry<Integer,Long>> allAges = getAges(region);
        return getTop3(allAges);
    }

    /**
     * Given a list of region names, call {@link #iteratorFactory} to get an iterator for each region and return
     * the 3 most common ages across all regions in the format specified by {@link #OUTPUT_FORMAT}.
     * We expect you to make use of all cores in the machine, specified by {@link #CORES).
     */
    public String[] top3Ages(List<String> regionNames) {
        Executor executor = Executors.newFixedThreadPool(CORES);
        CompletionService<List<Map.Entry<Integer, Long>>> completionService = new ExecutorCompletionService<>(executor);

        for (String region: regionNames) {
            completionService.submit(() -> getAges(region));
        }

        int received = 0;
        List<Map.Entry<Integer, Long>> ages = new ArrayList<>();

        while (received < regionNames.size()) {
            try {
                Future<List<Map.Entry<Integer, Long>>> resultFuture = completionService.take();
                List<Map.Entry<Integer, Long>> result = resultFuture.get();
                ages.addAll(result);
                received++;
            } catch (InterruptedException | ExecutionException | RuntimeException e) {
                e.printStackTrace();
                return new String[]{};
            }
        }

        Map<Integer, Long> merged =
                ages.stream()
                        .collect(Collectors.groupingBy(Map.Entry::getKey, Collectors.summingLong(Map.Entry::getValue)));
        List<Map.Entry<Integer, Long>> sorted =
                merged.entrySet().stream()
                        .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                        .collect(Collectors.toList());
        return getTop3(sorted);
    }


    /**
     * Given one region name, return a sorted list of (Age, Total) pairs of the region
     */
    private List<Map.Entry<Integer, Long>> getAges(String region) {
        try (Census.AgeInputIterator iterator = iteratorFactory.apply(region)) {
            Iterable<Integer> iterable = () -> iterator;
            Stream<Integer> streamInt = StreamSupport.stream(iterable.spliterator(), false);
            Map<Integer, Long> ageMap =
                    streamInt.filter(age -> age >= 0)
                            .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
            return ageMap.entrySet().stream()
                    .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                    .collect(Collectors.toList());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new ArrayList<>();
    }

    /**
     * Given a sorted list of (Age, Total) pairs, return the 3 most common ages in the format
     * specified by {@link #OUTPUT_FORMAT}.
     */
    private String[] getTop3(List<Map.Entry<Integer,Long>> allAges) {
        List<String> top3Ages = new ArrayList<>();

        if (!allAges.isEmpty()) {
            int position = 1;
            long previousTotal = allAges.get(0).getValue();
            top3Ages.add(String.format(OUTPUT_FORMAT, position, allAges.get(0).getKey(), allAges.get(0).getValue()));

            for ( int i = 1; i < allAges.size(); i++) {
                if (allAges.get(i).getValue() != previousTotal) {
                    position++;
                    if (position == 4) {
                        break;
                    }
                }
                top3Ages.add(String.format(OUTPUT_FORMAT, position, allAges.get(i).getKey(), allAges.get(i).getValue()));
                previousTotal = allAges.get(i).getValue();
            }
        }
        String[] result = new String[top3Ages.size()];
        return top3Ages.toArray(result);
    }

    /**
     * Implementations of this interface will return ages on call to {@link Iterator#next()}. They may open resources
     * when being instantiated created.
     */
    public interface AgeInputIterator extends Iterator<Integer>, Closeable {
    }
}
