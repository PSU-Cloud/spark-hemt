/*
 * Copyright 2010-2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazonaws.samples;

import java.util.Arrays;
import java.util.Date;
import java.io.IOException;

import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.GetMetricStatisticsRequest;
import com.amazonaws.services.cloudwatch.model.GetMetricStatisticsResult;
import com.amazonaws.services.cloudwatch.model.ListMetricsRequest;
import com.amazonaws.services.cloudwatch.model.ListMetricsResult;
import com.amazonaws.services.cloudwatch.model.Metric;
import com.amazonaws.util.EC2MetadataUtils;

public class S3Sample {
    public static void main(String[] args) throws IOException, InterruptedException {
        while(true) {
            AmazonCloudWatch cw = AmazonCloudWatchClientBuilder.defaultClient();
            Dimension instanceDimension = new Dimension();
            instanceDimension.setName("InstanceId");
            instanceDimension.setValue(EC2MetadataUtils.getInstanceId());

            GetMetricStatisticsRequest request = new GetMetricStatisticsRequest()
                .withStartTime(new Date(new Date().getTime() - 300000))
                .withNamespace("AWS/EC2")
                .withPeriod(60 * 60)
                .withMetricName("CPUCreditBalance")
                .withStatistics("Average")
                .withDimensions(Arrays.asList(instanceDimension))
                .withEndTime(new Date());
            int credits = 0;
            try {
                GetMetricStatisticsResult result = cw.getMetricStatistics(request);
                credits= result.getDatapoints().get(0).getAverage().intValue();
                System.out.println("Got current credits: " + credits);
            } catch (Exception e) {
                System.out.println("Got exception: " + e.toString());
            }
            Thread.sleep(10*1000);    // sleep for 10 secs
        } 
    }
}
