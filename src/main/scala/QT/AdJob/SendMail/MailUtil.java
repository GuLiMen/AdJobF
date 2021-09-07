package QT.AdJob.SendMail;

/**
 * Project : AdJobF
 * Author  : zhilin.gao
 * Date    : 2021/9/7 9:25
 */



import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.activation.FileDataSource;
import javax.mail.*;
import javax.mail.internet.*;
import java.lang.reflect.Array;
import java.util.List;
import java.util.Properties;
import QT.AdJob.utils.Constant;
import org.apache.spark.sql.Row;
import org.omg.CORBA.Any;


public class MailUtil implements Runnable {
    private String email;// 收件人邮箱
    private String content;//内容
    private String filename;//附件
    private String subject;//主题
    private Boolean ishtml;//是否为html代码

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public Boolean getIshtml() {
        return ishtml;
    }

    public void setIshtml(Boolean ishtml) {
        this.ishtml = ishtml;
    }

    public MailUtil(Builder builder) {
        this.content = builder.content;
        this.filename = builder.filename;
        this.subject = builder.subject;
        this.email = builder.email;
        this.ishtml = builder.ishtml;
    }

    public static class Builder{
        private String email;// 收件人邮箱
        private String content="";//内容
        private String filename=null;//附件
        private String subject="WB智慧打线";//主题
        private Boolean ishtml = true;//是否为html代码

        public Builder setContent(String content_b){
            this.content = content_b;
            return this;
        }
        public Builder setFilename(String filename_b){
            this.filename = filename_b;
            return this;
        }
        public Builder setSubject(String subject_b){
            this.subject = subject_b;
            return this;
        }
        public Builder isHtml(){
            this.ishtml = true;
            return this;
        }

        public MailUtil buildWithEmail(String email_b) {
            this.email = email_b;
            return new MailUtil(this);
        }
    }



    public void run() {

        String from = Constant.MAILUSER();// 发件人电子邮箱
        String host = Constant.mailHost(); // 指定发送邮件的主机
        String port = Constant.mailPort();
        Properties properties = System.getProperties();// 获取系统属性
        properties.setProperty("mail.transport.protocol", "stmp");
        properties.setProperty("mail.smtp.host", host);// 设置邮件服务器
        properties.setProperty("mail.smtp.port", port);
//    properties.setProperty("mail.smtp.auth.login.disable", "true");
        properties.setProperty("mail.debug", "true");
        properties.setProperty("mail.smtp.auth", "true");// 打开认证
        properties.setProperty("mail.mime.address.strict", "false");

        try {
            //QQ邮箱需要下面这段代码，163邮箱不需要
//            MailSSLSocketFactory sf = new MailSSLSocketFactory();
//            sf.setTrustAllHosts(true);
//            properties.put("mail.smtp.ssl.enable", "true");
//            properties.put("mail.smtp.ssl.socketFactory", sf);

            //获取默认session对象
            Session session = Session.getDefaultInstance(properties, new Authenticator() {
                public PasswordAuthentication getPasswordAuthentication() {
                    return new PasswordAuthentication(Constant.MAILUSER(), Constant.MAILPWD()); // 发件人邮箱账号、授权码
                }
            });

            // 创建邮件对象
            Message message = new MimeMessage(session);
            // 设置发件人
            message.setFrom(new InternetAddress(from));
            // 设置接收人
            message.addRecipient(Message.RecipientType.TO, new InternetAddress(email));
            // 设置邮件主题
            message.setSubject(subject);
            // 创建消息部分
            BodyPart messageBodyPart = new MimeBodyPart();
            // 消息
            if (ishtml){
                messageBodyPart.setContent(content,"text/html;charset=UTF-8");
            }else {
                messageBodyPart.setText(content);
            }
            // 创建多重消息
            Multipart multipart = new MimeMultipart();
            // 设置文本消息部分
            multipart.addBodyPart(messageBodyPart);
            // 附件部分
            if (filename!=null){
                BodyPart messageAttachmentPart = new MimeBodyPart();
                DataSource source = new FileDataSource(filename);
                messageAttachmentPart.setDataHandler(new DataHandler(source));
                messageAttachmentPart.setFileName(filename);
                multipart.addBodyPart(messageAttachmentPart);
            }
            message.setContent(multipart);
            // 发送邮件
            Transport.send(message);
        } catch (Exception e) {
            e.printStackTrace();
//        LogUtil.error("邮件发送失败"+subject,e);
        }
    }

    public static StringBuilder getTableStart(Row[] head, String title){

        StringBuilder table=new StringBuilder();

        table.append("<p>各位好：</p>\n");
        table.append("<p>下表是智慧打线本次检测到的异常值，请关注，有问题及时沟通！</p>\n");
        table.append("<h2>"+title+"</h2>\n");
        table.append("<table border=1> <tr> <th>设备号</th>  <th>机种</th>  <th>时间</th> <th>代码</th> <th>描述</th> </tr>\n");

        for (int i=0; i < head.length; i++){
            System.out.println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$" + head[i]);
            table.append("<tr> <td>" + head[i].get(0) + "</td> <td>" + head[i].get(1) + "</td> <td>" + head[i].get(2) + "</td> <td>" + head[i].get(3) + "</td> <td>" + head[i].get(4) + "</td> </tr> ");
        }
        table.append("</table>");
        System.out.println(table.toString());
        return table;
    }

}
