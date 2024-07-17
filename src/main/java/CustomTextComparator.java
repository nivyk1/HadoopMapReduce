import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CustomTextComparator extends WritableComparator {
    public CustomTextComparator() {
        super(Text.class, true);
    }
    @Override
    public int compare(WritableComparable t1, WritableComparable t2) {

       // npmi w1 w2 decade
        String []t1tmp =t1.toString().split(" ");
        double t1npmi= (Double.parseDouble(t1tmp[0]));
        String []t2tmp =t2.toString().split(" ");
        double t2npmi= (Double.parseDouble(t2tmp[0]));
    return -Double.compare(t1npmi,t2npmi);
    }


}
