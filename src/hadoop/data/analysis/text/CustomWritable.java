package hadoop.data.analysis.text;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CustomWritable implements Writable {

    private String questionOne = "";
    private String questionTwo = "";
    private String questionThree = "";
    private String questionFour = "";
    private String questionFive = "";
    private String questionSix = "";
    private String questionSeven = "";
    private String questionEight = "";
    private String questionNine = "";
    private String questionNineCorrelation = "";
    private String fourTest = "";

    public CustomWritable() {}

    /**
     * Reads fields from file
     * @param dataInput Hadoop data input
     * @throws IOException
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        questionOne = WritableUtils.readString(dataInput);
        questionTwo = WritableUtils.readString(dataInput);
        questionThree = WritableUtils.readString(dataInput);
        questionFour = WritableUtils.readString(dataInput);
        questionFive = WritableUtils.readString(dataInput);
        questionSix = WritableUtils.readString(dataInput);
        questionSeven = WritableUtils.readString(dataInput);
        questionEight = WritableUtils.readString(dataInput);
        questionNine = WritableUtils.readString(dataInput);
        questionNineCorrelation = WritableUtils.readString(dataInput);
        fourTest = WritableUtils.readString(dataInput);
    }

    /**
     * Writes fields from file
     * @param dataOutput Hadoop data output
     * @throws IOException
     */

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        WritableUtils.writeString(dataOutput, questionOne);
        WritableUtils.writeString(dataOutput, questionTwo);
        WritableUtils.writeString(dataOutput, questionThree);
        WritableUtils.writeString(dataOutput, questionFour);
        WritableUtils.writeString(dataOutput, questionFive);
        WritableUtils.writeString(dataOutput, questionSix);
        WritableUtils.writeString(dataOutput, questionSeven);
        WritableUtils.writeString(dataOutput, questionEight);
        WritableUtils.writeString(dataOutput, questionNine);
        WritableUtils.writeString(dataOutput, questionNineCorrelation);
        WritableUtils.writeString(dataOutput, fourTest);
    }

    public String getQuestionOne() {return questionOne;}
    public void setQuestionOne(String questionOne) {this.questionOne = questionOne;}

    public String getQuestionTwo() {return questionTwo;}
    public void setQuestionTwo(String questionTwo) {this.questionTwo = questionTwo;}

    public String getQuestionThree() {return questionThree;}
    public void setQuestionThree(String questionThree) {this.questionThree = questionThree;}

    public String getQuestionFour() {return questionFour;}
    public void setQuestionFour(String questionFour) {this.questionFour = questionFour;}

    public String getQuestionFive() {return questionFive;}
    public void setQuestionFive(String questionFive) {this.questionFive = questionFive;}

    public String getQuestionSix() {return questionSix;}
    public void setQuestionSix(String questionSix) {this.questionSix = questionSix;}

    public String getQuestionSeven() {return questionSeven;}
    public void setQuestionSeven(String questionSeven) {this.questionSeven = questionSeven;}

    public String getQuestionEight() {return questionEight;}
    public void setQuestionEight(String questionEight) {this.questionEight = questionEight;}

    public String getQuestionNine() {return questionNine;}
    public void setQuestionNine(String questionNine) {this.questionNine = questionNine;}

    public String getQuestionNineCorrelation() { return questionNineCorrelation; }
    public void setQuestionNineCorrelation(String questionNineCorrelation) { this.questionNineCorrelation = questionNineCorrelation; }

    public String getFourTest() { return fourTest; }
    public void setFourTest(String fourTest) { this.fourTest = fourTest; }
}
