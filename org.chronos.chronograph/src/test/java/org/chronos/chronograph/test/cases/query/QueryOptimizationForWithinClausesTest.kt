package org.chronos.chronograph.test.cases.query

import org.chronos.chronograph.api.builder.query.CP
import org.chronos.chronograph.test.base.AllChronoGraphBackendsTest
import org.junit.Test
import strikt.api.expectThat
import strikt.assertions.containsExactlyInAnyOrder

class QueryOptimizationForWithinClausesTest : AllChronoGraphBackendsTest() {

    @Test
    fun runTest() {
        graph.indexManagerOnMaster.create().stringIndex().onVertexProperty("entityClass").acrossAllTimestamps().build()
        graph.indexManagerOnMaster.create().stringIndex().onVertexProperty("kind").acrossAllTimestamps().build()
        graph.indexManagerOnMaster.reindexAll()

        graph.tx().createThreadedTx().use { txGraph ->
            val allEntityClasses = setOf(
                "t:075f731e-76f5-4f11-a7ea-2ce23f5b42a9",
                "t:0abbc622-6e8a-4cb7-837a-f8eceeda3591",
                "t:0af0bd06-bef1-4fad-8210-0913fe049ac7",
                "t:0e65cd3c-aebc-4491-b6d4-ceb19c9e4611",
                "t:18202271-f488-4e43-8226-d985c46d0c66",
                "t:1e701175-116f-4e10-8e07-a834364ac99d",
                "t:36299456-add1-41e2-9713-a7726e0f9cdb",
                "t:37fb57e7-80c3-4baa-b6c1-2c9fbfa39ac3",
                "t:3ae09c86-4194-47e5-a457-6ef8c3c3b655",
                "t:3d616dd8-d97d-4917-bfb2-47d1fc59a272",
                "t:452d188e-8584-44e0-9da6-87633e02f98f",
                "t:4532a012-bd5c-437f-9a6b-b3c7266aed35",
                "t:4b9c4cd3-da3c-45f2-9837-3fa1b47e9e9f",
                "t:4c4d17ec-2977-4847-be69-ac9b51738b07",
                "t:51bc7635-39ae-4def-8deb-8419dda62c7b",
                "t:5bb11078-b254-4dca-b2d5-deb521e4d39f",
                "t:5bf1b124-402d-408f-9c6f-3f5b6d0b24eb",
                "t:5db533c5-252c-4227-8f72-369ba9a15c8d",
                "t:665b4123-529a-4d8f-a083-17742c227674",
                "t:7602aff2-1a76-41b0-88e0-58cd6c8bcf57",
                "t:772c644f-abbf-4400-a6a3-ea982d9e854c",
                "t:81bd079a-a3c0-47a8-b691-cc7fa851b134",
                "t:8c57e217-3bf9-41b3-afb9-b4f288612ae6",
                "t:913359d0-08fd-4748-b11e-2577515e6fee",
                "t:96069366-280d-4775-80f2-3b4a84a82034",
                "t:9da6382f-b12c-4df3-8857-2e6d665e089c",
                "t:a37ef201-fea7-4885-aeeb-6de32dacb2ad",
                "t:afc3aab1-80f8-4c41-a9cd-d6761765164f",
                "t:b2f58e90-8823-461f-87ad-c3e04bed828a",
                "t:b6a0e638-e27d-4b19-a35a-ed16c74dc2fd",
                "t:b74adf4d-fe86-4b9c-ae62-98d23c3455a7",
                "t:bcb5ad5b-6f1c-432e-83f4-f400a48acda8",
                "t:bff2900a-9123-458a-a3b8-cef792029161",
                "t:c01880ad-0881-4a7c-bcef-879db41a732c",
                "t:c050e0c5-8002-4287-ae51-1ac745fa2741",
                "t:c0b19cd5-7640-4ca0-982f-adb49ba0a110",
                "t:c974172d-e2d4-45f4-8c6a-b3c11b541e00",
                "t:deafb29a-8cee-4298-ac50-7158bf735d7c",
                "t:ec924179-e409-45c8-9b69-99d160d0f635",
                "t:ee92f957-286e-47d0-a90e-f64ae4708e65",
                "t:f1999309-6c1e-45e3-b008-a2c661cdf0da",
                "t:fbd4d798-40ed-4464-911f-2045de81bb59",
            )

            for (entityClass in allEntityClasses) {
                repeat(10) { i ->
                    val yesNo = if (i % 2 == 0) {
                        "Yes"
                    } else {
                        "No"
                    }
                    txGraph.addVertex(
                        "kind", "entity",
                        "entityClass", entityClass,
                        "prop", yesNo,
                        "name", "${entityClass}#${i}"
                    )
                }
            }

            txGraph.tx().commit()
        }

        graph.tx().createThreadedTx().use { txGraph ->
            // create a query with multiple "within" clauses, most
            // of which are useless because they're overruled by
            // only one of them
            val vertexNames = txGraph.traversal().V()
                .has(
                    "entityClass", CP.within(
                        "t:1e701175-116f-4e10-8e07-a834364ac99d",
                        "t:c0b19cd5-7640-4ca0-982f-adb49ba0a110",
                        "t:18202271-f488-4e43-8226-d985c46d0c66",
                        "t:913359d0-08fd-4748-b11e-2577515e6fee",
                        "t:3d616dd8-d97d-4917-bfb2-47d1fc59a272",
                    )
                )
                .has(
                    "entityClass", CP.within(
                        "t:deafb29a-8cee-4298-ac50-7158bf735d7c",
                        "t:8c57e217-3bf9-41b3-afb9-b4f288612ae6",
                        "t:4b9c4cd3-da3c-45f2-9837-3fa1b47e9e9f",
                        "t:1e701175-116f-4e10-8e07-a834364ac99d",
                        "t:772c644f-abbf-4400-a6a3-ea982d9e854c",
                        "t:9da6382f-b12c-4df3-8857-2e6d665e089c",
                        "t:c0b19cd5-7640-4ca0-982f-adb49ba0a110",
                        "t:b74adf4d-fe86-4b9c-ae62-98d23c3455a7",
                        "t:51bc7635-39ae-4def-8deb-8419dda62c7b",
                        "t:afc3aab1-80f8-4c41-a9cd-d6761765164f",
                        "t:ee92f957-286e-47d0-a90e-f64ae4708e65",
                        "t:3ae09c86-4194-47e5-a457-6ef8c3c3b655",
                        "t:a37ef201-fea7-4885-aeeb-6de32dacb2ad",
                        "t:075f731e-76f5-4f11-a7ea-2ce23f5b42a9",
                        "t:665b4123-529a-4d8f-a083-17742c227674",
                        "t:b2f58e90-8823-461f-87ad-c3e04bed828a",
                        "t:37fb57e7-80c3-4baa-b6c1-2c9fbfa39ac3",
                        "t:4c4d17ec-2977-4847-be69-ac9b51738b07",
                        "t:5db533c5-252c-4227-8f72-369ba9a15c8d",
                        "t:c01880ad-0881-4a7c-bcef-879db41a732c",
                        "t:81bd079a-a3c0-47a8-b691-cc7fa851b134",
                        "t:0abbc622-6e8a-4cb7-837a-f8eceeda3591",
                        "t:7602aff2-1a76-41b0-88e0-58cd6c8bcf57",
                        "t:36299456-add1-41e2-9713-a7726e0f9cdb",
                        "t:c974172d-e2d4-45f4-8c6a-b3c11b541e00",
                        "t:f1999309-6c1e-45e3-b008-a2c661cdf0da",
                        "t:18202271-f488-4e43-8226-d985c46d0c66",
                        "t:96069366-280d-4775-80f2-3b4a84a82034",
                        "t:fbd4d798-40ed-4464-911f-2045de81bb59",
                        "t:c050e0c5-8002-4287-ae51-1ac745fa2741",
                        "t:bff2900a-9123-458a-a3b8-cef792029161",
                        "t:b6a0e638-e27d-4b19-a35a-ed16c74dc2fd",
                        "t:5bf1b124-402d-408f-9c6f-3f5b6d0b24eb",
                        "t:ec924179-e409-45c8-9b69-99d160d0f635",
                        "t:4532a012-bd5c-437f-9a6b-b3c7266aed35",
                        "t:0e65cd3c-aebc-4491-b6d4-ceb19c9e4611",
                        "t:0af0bd06-bef1-4fad-8210-0913fe049ac7",
                        "t:913359d0-08fd-4748-b11e-2577515e6fee",
                        "t:5bb11078-b254-4dca-b2d5-deb521e4d39f",
                        "t:3d616dd8-d97d-4917-bfb2-47d1fc59a272",
                        "t:452d188e-8584-44e0-9da6-87633e02f98f",
                        "t:bcb5ad5b-6f1c-432e-83f4-f400a48acda8",
                    )
                )
                .has("entityClass", CP.within("t:913359d0-08fd-4748-b11e-2577515e6fee"))
                .has("kind", CP.eq("entity"))
                .has("prop", CP.eqIgnoreCase("Yes"))
                .fold()
                .map { t -> t.get().map { it.value<String>("name") }.toSet() }
                .next()

            expectThat(vertexNames).containsExactlyInAnyOrder(
                "t:913359d0-08fd-4748-b11e-2577515e6fee#0",
                "t:913359d0-08fd-4748-b11e-2577515e6fee#2",
                "t:913359d0-08fd-4748-b11e-2577515e6fee#4",
                "t:913359d0-08fd-4748-b11e-2577515e6fee#6",
                "t:913359d0-08fd-4748-b11e-2577515e6fee#8",
            )
        }
    }

}