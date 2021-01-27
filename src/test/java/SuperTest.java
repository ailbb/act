/**
 * Created by Wz on 6/19/2020.
 */
public class SuperTest {
    public static abstract class parent {
        private String pname = "测试！";

        public String getPname() {
            return pname;
        }

        public void setPname(String pname) {
            this.pname = pname;
        }
    }

    static class A extends parent {
        @Override
        public String getPname() {
            this.setPname("你好啊！");
            return super.getPname();
        }
    }

    public static void main(String[] args) {
        parent a = new A();
        System.out.println(a.getPname());
    }
}
