package hadoop.data.analysis.text;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CustomWritable implements Writable {

    //define strings with initial 0 values to avoid number format and null pointer exceptions
    //because not all files will contain question data
    private String questionOne = "0:0";
    private String questionTwo = "0:0";
    private String questionThree = "0";
    private String questionFour = "0:0";
    private String questionFive = "0";
    private String questionSix = "0";
    private String questionSeven = "0:0";
    private String questionEight = "0:0";
    private String questionNine = "0:0:0:0:0:0:0:0";
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
//        questionNine = WritableUtils.readString(dataInput);
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
//        WritableUtils.writeString(dataOutput, questionNine);
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

    public String getFourTest() {
        return fourTest;
    }

    public void setFourTest(String fourTest) {
        this.fourTest = fourTest;
    }
}
